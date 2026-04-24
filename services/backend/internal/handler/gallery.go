package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

type galleryMetric string

const (
	galleryMetricCosine          galleryMetric = "cosine"
	galleryMetricWeightedJaccard galleryMetric = "weighted_jaccard"
)

type galleryOwner struct {
	OwnerUserID       int
	Username          string
	SnapshotPath      *string
	SnapshotUpdatedAt *time.Time
}

type galleryGraphCard struct {
	OwnerUserID       int                     `json:"owner_user_id"`
	Username          string                  `json:"username"`
	FilesCount        int                     `json:"files_count"`
	TagsCount         int                     `json:"tags_count"`
	Similarity        float64                 `json:"similarity"`
	Metric            string                  `json:"metric"`
	SnapshotPath      *string                 `json:"snapshot_path,omitempty"`
	SnapshotUpdatedAt *time.Time              `json:"snapshot_updated_at,omitempty"`
	SnapshotSummary   *gallerySnapshotSummary `json:"snapshot_summary,omitempty"`
}

type listGalleryResponse struct {
	Metric string             `json:"metric"`
	Total  int                `json:"total"`
	Limit  int                `json:"limit"`
	Offset int                `json:"offset"`
	Items  []galleryGraphCard `json:"items"`
}

type galleryFileTag struct {
	TagID       int64    `json:"tag_id"`
	DisplayName string   `json:"display_name"`
	Source      string   `json:"source"`
	AutoRank    *int16   `json:"auto_rank,omitempty"`
	Score       *float64 `json:"score,omitempty"`
}

type galleryFile struct {
	FileID    int              `json:"file_id"`
	FileName  string           `json:"file_name"`
	Status    string           `json:"status"`
	TopTag    string           `json:"top_tag"`
	Tags      []galleryFileTag `json:"tags"`
	CreatedAt string           `json:"created_at"`
}

type galleryGraphDetailResponse struct {
	OwnerUserID       int           `json:"owner_user_id"`
	Username          string        `json:"username"`
	ViewerUserID      int           `json:"viewer_user_id"`
	CanEdit           bool          `json:"can_edit"`
	IsPublic          bool          `json:"is_public"`
	Metric            string        `json:"metric"`
	Similarity        float64       `json:"similarity"`
	SnapshotPath      *string       `json:"snapshot_path,omitempty"`
	SnapshotUpdatedAt *time.Time    `json:"snapshot_updated_at,omitempty"`
	Files             []galleryFile `json:"files"`
}

type viewWriteResponse struct {
	OwnerUserID  int    `json:"owner_user_id"`
	ViewerUserID int    `json:"viewer_user_id"`
	Status       string `json:"status"`
}

type leaderboardItem struct {
	OwnerUserID int    `json:"owner_user_id"`
	Username    string `json:"username"`
	Views       int    `json:"views"`
	Downloads   int    `json:"downloads"`
}

type leaderboardResponse struct {
	Period string            `json:"period"`
	Total  int               `json:"total"`
	Limit  int               `json:"limit"`
	Offset int               `json:"offset"`
	Items  []leaderboardItem `json:"items"`
}

func normalizeGalleryMetric(value string) galleryMetric {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(galleryMetricWeightedJaccard):
		return galleryMetricWeightedJaccard
	default:
		return galleryMetricCosine
	}
}

func parsePositiveQueryInt(raw string, fallback int, maxValue int) int {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	if parsed > maxValue {
		return maxValue
	}
	return parsed
}

func parseNonNegativeQueryInt(raw string, fallback int, maxValue int) int {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	if parsed > maxValue {
		return maxValue
	}
	return parsed
}

func cosineSimilarity(left map[int64]float64, right map[int64]float64) float64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}

	var dot float64
	var leftNorm float64
	var rightNorm float64

	for tagID, weight := range left {
		leftNorm += weight * weight
		rightWeight := right[tagID]
		dot += weight * rightWeight
	}
	for _, weight := range right {
		rightNorm += weight * weight
	}

	if leftNorm <= 0 || rightNorm <= 0 {
		return 0
	}

	value := dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm))
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func weightedJaccardSimilarity(left map[int64]float64, right map[int64]float64) float64 {
	if len(left) == 0 && len(right) == 0 {
		return 1
	}

	unionKeys := make(map[int64]struct{}, len(left)+len(right))
	for tagID := range left {
		unionKeys[tagID] = struct{}{}
	}
	for tagID := range right {
		unionKeys[tagID] = struct{}{}
	}

	var numerator float64
	var denominator float64
	for tagID := range unionKeys {
		leftWeight := left[tagID]
		rightWeight := right[tagID]
		numerator += math.Min(leftWeight, rightWeight)
		denominator += math.Max(leftWeight, rightWeight)
	}

	if denominator <= 0 {
		return 0
	}
	value := numerator / denominator
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func similarityByMetric(metric galleryMetric, left map[int64]float64, right map[int64]float64) float64 {
	switch metric {
	case galleryMetricWeightedJaccard:
		return weightedJaccardSimilarity(left, right)
	default:
		return cosineSimilarity(left, right)
	}
}

func (fh *FileHandler) loadUserTagWeights(ctx *http.Request, userID int) (map[int64]float64, error) {
	var totalFiles int
	err := fh.FileService.DB.QueryRow(ctx.Context(), `
		SELECT COUNT(DISTINCT f.id)
		FROM files f
		JOIN file_tags ft ON ft.file_id = f.id
		WHERE f.user_id = $1
	`, userID).Scan(&totalFiles)
	if err != nil {
		return nil, err
	}
	if totalFiles <= 0 {
		return map[int64]float64{}, nil
	}

	rows, err := fh.FileService.DB.Query(ctx.Context(), `
		SELECT ft.tag_id, COUNT(DISTINCT ft.file_id)
		FROM file_tags ft
		JOIN files f ON f.id = ft.file_id
		WHERE f.user_id = $1
		GROUP BY ft.tag_id
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	weights := make(map[int64]float64)
	for rows.Next() {
		var tagID int64
		var taggedFiles int
		if err := rows.Scan(&tagID, &taggedFiles); err != nil {
			return nil, err
		}
		weights[tagID] = float64(taggedFiles) / float64(totalFiles)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return weights, nil
}

func (fh *FileHandler) loadPublicGalleryOwners(ctx *http.Request, viewerUserID int) ([]galleryOwner, error) {
	rows, err := fh.FileService.DB.Query(ctx.Context(), `
		SELECT u.id, u.username, ugp.gallery_snapshot_path, ugp.gallery_snapshot_updated_at
		FROM user_graph_preferences ugp
		JOIN users u ON u.id = ugp.user_id
		WHERE ugp.gallery_visibility = 'public'
		  AND ugp.user_id <> $1
		ORDER BY u.username ASC
	`, viewerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	owners := make([]galleryOwner, 0)
	for rows.Next() {
		var owner galleryOwner
		if err := rows.Scan(&owner.OwnerUserID, &owner.Username, &owner.SnapshotPath, &owner.SnapshotUpdatedAt); err != nil {
			return nil, err
		}
		owners = append(owners, owner)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return owners, nil
}

func (fh *FileHandler) loadUserGraphCounts(ctx *http.Request, userID int) (int, int, error) {
	return fh.loadUserGraphCountsFromContext(ctx.Context(), userID)
}

func (fh *FileHandler) loadUserGraphCountsFromContext(ctx context.Context, userID int) (int, int, error) {
	var filesCount int
	var tagsCount int
	err := fh.FileService.DB.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM files WHERE user_id = $1) AS files_count,
			(SELECT COUNT(DISTINCT ft.tag_id)
			 FROM file_tags ft
			 JOIN files f ON f.id = ft.file_id
			 WHERE f.user_id = $1) AS tags_count
	`, userID).Scan(&filesCount, &tagsCount)
	if err != nil {
		return 0, 0, err
	}
	return filesCount, tagsCount, nil
}

func (fh *FileHandler) handleListGalleryGraphs(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	metric := normalizeGalleryMetric(r.URL.Query().Get("metric"))
	limit := parsePositiveQueryInt(r.URL.Query().Get("limit"), 20, 100)
	offset := parseNonNegativeQueryInt(r.URL.Query().Get("offset"), 0, 5000)

	viewerWeights, err := fh.loadUserTagWeights(r, user.ID)
	if err != nil {
		return err
	}

	owners, err := fh.loadPublicGalleryOwners(r, user.ID)
	if err != nil {
		return err
	}

	items := make([]galleryGraphCard, 0, len(owners))
	for _, owner := range owners {
		ownerWeights, err := fh.loadUserTagWeights(r, owner.OwnerUserID)
		if err != nil {
			return err
		}
		var snapshotSummary *gallerySnapshotSummary
		filesCount := 0
		tagsCount := 0

		if owner.SnapshotPath != nil && strings.TrimSpace(*owner.SnapshotPath) != "" {
			snapshotSummary, err = fh.loadGallerySnapshotSummary(
				r.Context(),
				owner.OwnerUserID,
				*owner.SnapshotPath,
				owner.SnapshotUpdatedAt,
			)
			if err != nil {
				log.Printf("Failed to load gallery snapshot for owner %d: %v", owner.OwnerUserID, err)
			}
		}

		if snapshotSummary != nil {
			filesCount = snapshotSummary.FilesCount
			tagsCount = snapshotSummary.TagsCount
		} else {
			filesCount, tagsCount, err = fh.loadUserGraphCounts(r, owner.OwnerUserID)
			if err != nil {
				return err
			}
		}

		items = append(items, galleryGraphCard{
			OwnerUserID:       owner.OwnerUserID,
			Username:          owner.Username,
			FilesCount:        filesCount,
			TagsCount:         tagsCount,
			Similarity:        similarityByMetric(metric, viewerWeights, ownerWeights),
			Metric:            string(metric),
			SnapshotPath:      owner.SnapshotPath,
			SnapshotUpdatedAt: owner.SnapshotUpdatedAt,
			SnapshotSummary:   snapshotSummary,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Similarity == items[j].Similarity {
			if items[i].FilesCount == items[j].FilesCount {
				return items[i].Username < items[j].Username
			}
			return items[i].FilesCount > items[j].FilesCount
		}
		return items[i].Similarity > items[j].Similarity
	})

	total := len(items)
	start := offset
	if start > total {
		start = total
	}
	end := start + limit
	if end > total {
		end = total
	}

	return writeJSON(w, http.StatusOK, listGalleryResponse{
		Metric: string(metric),
		Total:  total,
		Limit:  limit,
		Offset: offset,
		Items:  items[start:end],
	})
}

func (fh *FileHandler) loadGalleryOwner(ctx *http.Request, ownerUserID int) (galleryOwner, string, error) {
	var owner galleryOwner
	var visibility string
	err := fh.FileService.DB.QueryRow(ctx.Context(), `
		SELECT u.id, u.username, ugp.gallery_snapshot_path, ugp.gallery_snapshot_updated_at, ugp.gallery_visibility
		FROM users u
		LEFT JOIN user_graph_preferences ugp ON ugp.user_id = u.id
		WHERE u.id = $1
	`, ownerUserID).Scan(&owner.OwnerUserID, &owner.Username, &owner.SnapshotPath, &owner.SnapshotUpdatedAt, &visibility)
	if err != nil {
		return galleryOwner{}, "", err
	}
	return owner, visibility, nil
}

func (fh *FileHandler) handleGetGalleryGraph(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	ownerUserID, err := parsePathInt(chi.URLParam(r, "owner_user_id"), "owner_user_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	metric := normalizeGalleryMetric(r.URL.Query().Get("metric"))
	owner, visibility, err := fh.loadGalleryOwner(r, ownerUserID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Owner not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	isOwner := user.ID == ownerUserID
	isPublic := strings.EqualFold(strings.TrimSpace(visibility), "public")
	if !isOwner && !isPublic {
		http.Error(w, "Graph not published", http.StatusNotFound)
		return nil
	}

	viewerWeights, err := fh.loadUserTagWeights(r, user.ID)
	if err != nil {
		return err
	}
	ownerWeights, err := fh.loadUserTagWeights(r, ownerUserID)
	if err != nil {
		return err
	}

	rows, err := fh.FileService.DB.Query(r.Context(), `
		SELECT id, file_name, status, COALESCE(tag, ''), created_at
		FROM files
		WHERE user_id = $1
		ORDER BY created_at DESC
	`, ownerUserID)
	if err != nil {
		return err
	}
	defer rows.Close()

	files := make([]galleryFile, 0)
	fileIDs := make([]int, 0)
	for rows.Next() {
		var file galleryFile
		var createdAt time.Time
		if err := rows.Scan(&file.FileID, &file.FileName, &file.Status, &file.TopTag, &createdAt); err != nil {
			return err
		}
		file.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		files = append(files, file)
		fileIDs = append(fileIDs, file.FileID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	tagsByFileID := make(map[int][]galleryFileTag, len(fileIDs))
	if len(fileIDs) > 0 {
		tagRows, err := fh.FileService.DB.Query(r.Context(), `
			SELECT ft.file_id, t.id, t.display_name, ft.source, ft.auto_rank, ft.score
			FROM file_tags ft
			JOIN tags t ON t.id = ft.tag_id
			JOIN files f ON f.id = ft.file_id
			WHERE f.user_id = $1
			ORDER BY ft.file_id ASC, ft.source ASC, ft.auto_rank ASC NULLS LAST, t.display_name ASC
		`, ownerUserID)
		if err != nil {
			return err
		}
		defer tagRows.Close()

		for tagRows.Next() {
			var fileID int
			var tag galleryFileTag
			if err := tagRows.Scan(&fileID, &tag.TagID, &tag.DisplayName, &tag.Source, &tag.AutoRank, &tag.Score); err != nil {
				return err
			}
			tagsByFileID[fileID] = append(tagsByFileID[fileID], tag)
		}
		if err := tagRows.Err(); err != nil {
			return err
		}
	}

	for idx := range files {
		files[idx].Tags = tagsByFileID[files[idx].FileID]
	}

	return writeJSON(w, http.StatusOK, galleryGraphDetailResponse{
		OwnerUserID:       owner.OwnerUserID,
		Username:          owner.Username,
		ViewerUserID:      user.ID,
		CanEdit:           isOwner,
		IsPublic:          isPublic,
		Metric:            string(metric),
		Similarity:        similarityByMetric(metric, viewerWeights, ownerWeights),
		SnapshotPath:      owner.SnapshotPath,
		SnapshotUpdatedAt: owner.SnapshotUpdatedAt,
		Files:             files,
	})
}

func (fh *FileHandler) handleWriteGalleryView(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	ownerUserID, err := parsePathInt(chi.URLParam(r, "owner_user_id"), "owner_user_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}
	if ownerUserID == user.ID {
		return writeJSON(w, http.StatusOK, viewWriteResponse{OwnerUserID: ownerUserID, ViewerUserID: user.ID, Status: "skipped_self"})
	}

	_, visibility, err := fh.loadGalleryOwner(r, ownerUserID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Owner not found", http.StatusNotFound)
			return nil
		}
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(visibility), "public") {
		http.Error(w, "Graph not published", http.StatusNotFound)
		return nil
	}

	nowUTC := time.Now().UTC()
	_, err = fh.FileService.DB.Exec(r.Context(), `
		INSERT INTO graph_unique_views (owner_user_id, viewer_user_id, viewed_at, view_date_utc)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (owner_user_id, viewer_user_id, view_date_utc)
		DO UPDATE SET viewed_at = EXCLUDED.viewed_at
	`, ownerUserID, user.ID, nowUTC, nowUTC.Format("2006-01-02"))
	if err != nil {
		return err
	}

	return writeJSON(w, http.StatusOK, viewWriteResponse{OwnerUserID: ownerUserID, ViewerUserID: user.ID, Status: "recorded"})
}

func (fh *FileHandler) handleDownloadGalleryFile(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	ownerUserID, err := parsePathInt(chi.URLParam(r, "owner_user_id"), "owner_user_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}
	fileID, err := parsePathInt(chi.URLParam(r, "file_id"), "file_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	_, visibility, err := fh.loadGalleryOwner(r, ownerUserID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Owner not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	isOwner := ownerUserID == user.ID
	isPublic := strings.EqualFold(strings.TrimSpace(visibility), "public")
	if !isOwner && !isPublic {
		http.Error(w, "Graph not published", http.StatusNotFound)
		return nil
	}

	f, err := fh.FileService.GetFileByID(r.Context(), fileID, ownerUserID)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return nil
	}

	filerURL := fh.FilerURL + f.FilePath
	resp, err := http.Get(filerURL)
	if err != nil {
		http.Error(w, "Failed to download from filer", http.StatusInternalServerError)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		http.Error(w, fmt.Sprintf("Filer error: %s", string(bodyBytes)), resp.StatusCode)
		return nil
	}

	w.Header().Set("Content-Type", f.FileType)
	downloadName := url.PathEscape(f.FileName)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", downloadName))
	w.WriteHeader(http.StatusOK)

	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("Error while copying gallery file to response: %v", err)
		return nil
	}

	// Do not count owner's own downloads in leaderboard counters.
	if !isOwner {
		nowUTC := time.Now().UTC()
		_, err = fh.FileService.DB.Exec(r.Context(), `
			INSERT INTO graph_file_downloads (owner_user_id, viewer_user_id, file_id, downloaded_at, download_date_utc)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (owner_user_id, viewer_user_id, file_id, download_date_utc)
			DO UPDATE SET downloaded_at = EXCLUDED.downloaded_at
		`, ownerUserID, user.ID, fileID, nowUTC, nowUTC.Format("2006-01-02"))
		if err != nil {
			// Keep file delivery successful even if analytics write failed.
			log.Printf("Failed to persist gallery file download stat: %v", err)
		}
	}
	return nil
}

func normalizeLeaderboardPeriod(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "month":
		return "month"
	case "year":
		return "year"
	case "all":
		return "all"
	default:
		return "week"
	}
}

func periodStartUTC(period string, now time.Time) *time.Time {
	now = now.UTC()
	var start time.Time
	switch period {
	case "month":
		start = now.AddDate(0, -1, 0)
		return &start
	case "year":
		start = now.AddDate(-1, 0, 0)
		return &start
	case "all":
		return nil
	default:
		start = now.AddDate(0, 0, -7)
		return &start
	}
}

func (fh *FileHandler) handleGetLeaderboardGraphs(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	period := normalizeLeaderboardPeriod(r.URL.Query().Get("period"))
	limit := parsePositiveQueryInt(r.URL.Query().Get("limit"), 20, 100)
	offset := parseNonNegativeQueryInt(r.URL.Query().Get("offset"), 0, 5000)
	start := periodStartUTC(period, time.Now().UTC())

	var rows pgx.Rows
	var err error
	if start == nil {
		rows, err = fh.FileService.DB.Query(r.Context(), `
			SELECT
				u.id AS owner_user_id,
				u.username,
				COALESCE(v.views, 0)::int AS views,
				COALESCE(d.downloads, 0)::int AS downloads
			FROM user_graph_preferences ugp
			JOIN users u ON u.id = ugp.user_id
			LEFT JOIN (
				SELECT owner_user_id, COUNT(*)::int AS views
				FROM graph_unique_views
				GROUP BY owner_user_id
			) v ON v.owner_user_id = u.id
			LEFT JOIN (
				SELECT owner_user_id, COUNT(*)::int AS downloads
				FROM graph_file_downloads
				GROUP BY owner_user_id
			) d ON d.owner_user_id = u.id
			WHERE ugp.gallery_visibility = 'public'
			  AND (COALESCE(v.views, 0) > 0 OR COALESCE(d.downloads, 0) > 0)
			ORDER BY views DESC, downloads DESC, u.username ASC
		`)
	} else {
		rows, err = fh.FileService.DB.Query(r.Context(), `
			SELECT
				u.id AS owner_user_id,
				u.username,
				COALESCE(v.views, 0)::int AS views,
				COALESCE(d.downloads, 0)::int AS downloads
			FROM user_graph_preferences ugp
			JOIN users u ON u.id = ugp.user_id
			LEFT JOIN (
				SELECT owner_user_id, COUNT(*)::int AS views
				FROM graph_unique_views
				WHERE viewed_at >= $1
				GROUP BY owner_user_id
			) v ON v.owner_user_id = u.id
			LEFT JOIN (
				SELECT owner_user_id, COUNT(*)::int AS downloads
				FROM graph_file_downloads
				WHERE downloaded_at >= $1
				GROUP BY owner_user_id
			) d ON d.owner_user_id = u.id
			WHERE ugp.gallery_visibility = 'public'
			  AND (COALESCE(v.views, 0) > 0 OR COALESCE(d.downloads, 0) > 0)
			ORDER BY views DESC, downloads DESC, u.username ASC
		`, *start)
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	items := make([]leaderboardItem, 0)
	for rows.Next() {
		var item leaderboardItem
		if err := rows.Scan(&item.OwnerUserID, &item.Username, &item.Views, &item.Downloads); err != nil {
			return err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	total := len(items)
	startIndex := offset
	if startIndex > total {
		startIndex = total
	}
	endIndex := startIndex + limit
	if endIndex > total {
		endIndex = total
	}

	return writeJSON(w, http.StatusOK, leaderboardResponse{
		Period: period,
		Total:  total,
		Limit:  limit,
		Offset: offset,
		Items:  items[startIndex:endIndex],
	})
}
