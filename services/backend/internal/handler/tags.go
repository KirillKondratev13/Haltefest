package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

const (
	maxTagsPerFile   = 10
	maxAutoTagsPerFile = 5
	maxManualTagLen  = 15
)

type fileTagResponseItem struct {
	TagID       int64    `json:"tag_id"`
	DisplayName string   `json:"display_name"`
	Source      string   `json:"source"`
	AutoRank    *int16   `json:"auto_rank,omitempty"`
	Score       *float64 `json:"score,omitempty"`
}

type fileTagsResponse struct {
	FileID int                   `json:"file_id"`
	Tags   []fileTagResponseItem `json:"tags"`
}

type addManualTagRequest struct {
	Tag string `json:"tag"`
}

type replaceFileTagsRequest struct {
	AutoTagIDs  []int64  `json:"auto_tag_ids"`
	ManualTags  []string `json:"manual_tags"`
}

func normalizeTagName(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	return strings.Join(strings.Fields(trimmed), " ")
}

func validateManualTag(value string) (string, error) {
	normalized := normalizeTagName(value)
	if normalized == "" {
		return "", errors.New("tag is required")
	}
	if utf8.RuneCountInString(normalized) > maxManualTagLen {
		return "", errors.New("manual tag exceeds 15 characters")
	}
	return normalized, nil
}

func normalizeUniqueInt64(values []int64) []int64 {
	if len(values) == 0 {
		return []int64{}
	}
	seen := make(map[int64]struct{}, len(values))
	result := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func normalizeUniqueManualTags(values []string) ([]string, error) {
	if len(values) == 0 {
		return []string{}, nil
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		normalized, err := validateManualTag(value)
		if err != nil {
			return nil, err
		}
		key := strings.ToLower(normalized)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, normalized)
	}
	return result, nil
}

func (fh *FileHandler) syncLegacyTopTag(r *http.Request, tx pgx.Tx, fileID int) error {
	var topTag *string
	err := tx.QueryRow(r.Context(), `
		SELECT t.display_name
		FROM file_tags ft
		JOIN tags t ON t.id = ft.tag_id
		WHERE ft.file_id = $1
		  AND ft.source = 'AUTO'
		  AND ft.auto_rank = 1
		LIMIT 1
	`, fileID).Scan(&topTag)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		topTag = nil
	}

	_, err = tx.Exec(r.Context(), `
		UPDATE files
		SET tag = $1
		WHERE id = $2
	`, topTag, fileID)
	return err
}

func (fh *FileHandler) ensureOwnedFile(ctx *http.Request, fileID int, userID int) error {
	var exists int
	err := fh.FileService.DB.QueryRow(ctx.Context(), `
		SELECT 1
		FROM files
		WHERE id = $1 AND user_id = $2
	`, fileID, userID).Scan(&exists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return pgx.ErrNoRows
		}
		return err
	}
	return nil
}

func (fh *FileHandler) loadFileTags(r *http.Request, fileID int) ([]fileTagResponseItem, error) {
	rows, err := fh.FileService.DB.Query(r.Context(), `
		SELECT t.id, t.display_name, ft.source, ft.auto_rank, ft.score
		FROM file_tags ft
		JOIN tags t ON t.id = ft.tag_id
		WHERE ft.file_id = $1
		ORDER BY ft.source ASC, ft.auto_rank ASC NULLS LAST, t.display_name ASC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]fileTagResponseItem, 0)
	for rows.Next() {
		var tag fileTagResponseItem
		if err := rows.Scan(&tag.TagID, &tag.DisplayName, &tag.Source, &tag.AutoRank, &tag.Score); err != nil {
			return nil, err
		}
		result = append(result, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (fh *FileHandler) resolveOrCreateManualTag(tx pgx.Tx, r *http.Request, userID int, displayName string) (int64, error) {
	normalized := strings.ToLower(displayName)
	var tagID int64
	err := tx.QueryRow(r.Context(), `
		SELECT id
		FROM tags
		WHERE normalized_name = $1
	`, normalized).Scan(&tagID)
	if err == nil {
		return tagID, nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}

	err = tx.QueryRow(r.Context(), `
		INSERT INTO tags (normalized_name, display_name, is_system, created_by_user_id)
		VALUES ($1, $2, FALSE, $3)
		ON CONFLICT (normalized_name)
		DO UPDATE SET display_name = EXCLUDED.display_name
		RETURNING id
	`, normalized, displayName, userID).Scan(&tagID)
	if err != nil {
		return 0, err
	}
	return tagID, nil
}

func (fh *FileHandler) handleGetFileTags(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	fileID, err := parsePathInt(chi.URLParam(r, "file_id"), "file_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	if err := fh.ensureOwnedFile(r, fileID, user.ID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "File not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	tags, err := fh.loadFileTags(r, fileID)
	if err != nil {
		return err
	}

	return writeJSON(w, http.StatusOK, fileTagsResponse{FileID: fileID, Tags: tags})
}

func (fh *FileHandler) handleAddManualFileTag(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	fileID, err := parsePathInt(chi.URLParam(r, "file_id"), "file_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	var request addManualTagRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return nil
	}

	manualTag, err := validateManualTag(request.Tag)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	tx, err := fh.FileService.DB.Begin(r.Context())
	if err != nil {
		return err
	}
	defer tx.Rollback(r.Context())

	var owned int
	err = tx.QueryRow(r.Context(), `
		SELECT 1
		FROM files
		WHERE id = $1 AND user_id = $2
		FOR UPDATE
	`, fileID, user.ID).Scan(&owned)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "File not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	var currentCount int
	err = tx.QueryRow(r.Context(), `
		SELECT COUNT(*)
		FROM file_tags
		WHERE file_id = $1
	`, fileID).Scan(&currentCount)
	if err != nil {
		return err
	}
	if currentCount >= maxTagsPerFile {
		http.Error(w, "tag limit reached (max 10)", http.StatusConflict)
		return nil
	}

	tagID, err := fh.resolveOrCreateManualTag(tx, r, user.ID, manualTag)
	if err != nil {
		return err
	}

	_, err = tx.Exec(r.Context(), `
		INSERT INTO file_tags (file_id, tag_id, source, auto_rank, score)
		VALUES ($1, $2, 'MANUAL', NULL, NULL)
		ON CONFLICT (file_id, tag_id) DO UPDATE
		SET source = EXCLUDED.source,
			auto_rank = NULL,
			score = NULL,
			updated_at = NOW()
	`, fileID, tagID)
	if err != nil {
		return err
	}

	if err := fh.syncLegacyTopTag(r, tx, fileID); err != nil {
		return err
	}

	if err := tx.Commit(r.Context()); err != nil {
		return err
	}

	tags, err := fh.loadFileTags(r, fileID)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, fileTagsResponse{FileID: fileID, Tags: tags})
}

func (fh *FileHandler) handleDeleteFileTag(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	fileID, err := parsePathInt(chi.URLParam(r, "file_id"), "file_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	tagID, err := strconv.ParseInt(strings.TrimSpace(chi.URLParam(r, "tag_id")), 10, 64)
	if err != nil || tagID <= 0 {
		http.Error(w, "invalid tag_id", http.StatusBadRequest)
		return nil
	}

	if err := fh.ensureOwnedFile(r, fileID, user.ID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "File not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	tx, err := fh.FileService.DB.Begin(r.Context())
	if err != nil {
		return err
	}
	defer tx.Rollback(r.Context())

	_, err = tx.Exec(r.Context(), `
		DELETE FROM file_tags
		WHERE file_id = $1 AND tag_id = $2
	`, fileID, tagID)
	if err != nil {
		return err
	}

	if err := fh.syncLegacyTopTag(r, tx, fileID); err != nil {
		return err
	}

	if err := tx.Commit(r.Context()); err != nil {
		return err
	}

	tags, err := fh.loadFileTags(r, fileID)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, fileTagsResponse{FileID: fileID, Tags: tags})
}

func (fh *FileHandler) handleReplaceFileTags(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	fileID, err := parsePathInt(chi.URLParam(r, "file_id"), "file_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	var request replaceFileTagsRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return nil
	}

	autoTagIDs := normalizeUniqueInt64(request.AutoTagIDs)
	manualTags, err := normalizeUniqueManualTags(request.ManualTags)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	if len(autoTagIDs) > maxAutoTagsPerFile {
		http.Error(w, "auto tag limit reached (max 5)", http.StatusBadRequest)
		return nil
	}
	if len(autoTagIDs)+len(manualTags) > maxTagsPerFile {
		http.Error(w, "tag limit reached (max 10)", http.StatusBadRequest)
		return nil
	}

	tx, err := fh.FileService.DB.Begin(r.Context())
	if err != nil {
		return err
	}
	defer tx.Rollback(r.Context())

	var owned int
	err = tx.QueryRow(r.Context(), `
		SELECT 1
		FROM files
		WHERE id = $1 AND user_id = $2
		FOR UPDATE
	`, fileID, user.ID).Scan(&owned)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "File not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	if len(autoTagIDs) > 0 {
		for _, tagID := range autoTagIDs {
			var exists int
			err = tx.QueryRow(r.Context(), `SELECT 1 FROM tags WHERE id = $1`, tagID).Scan(&exists)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					http.Error(w, "unknown auto tag id", http.StatusBadRequest)
					return nil
				}
				return err
			}
		}
	}

	manualTagIDs := make([]int64, 0, len(manualTags))
	for _, manualTag := range manualTags {
		tagID, err := fh.resolveOrCreateManualTag(tx, r, user.ID, manualTag)
		if err != nil {
			return err
		}
		manualTagIDs = append(manualTagIDs, tagID)
	}

	_, err = tx.Exec(r.Context(), `
		DELETE FROM file_tags
		WHERE file_id = $1
	`, fileID)
	if err != nil {
		return err
	}

	for index, tagID := range autoTagIDs {
		autoRank := int16(index + 1)
		_, err := tx.Exec(r.Context(), `
			INSERT INTO file_tags (file_id, tag_id, source, auto_rank, score)
			VALUES ($1, $2, 'AUTO', $3, NULL)
		`, fileID, tagID, autoRank)
		if err != nil {
			return err
		}
	}

	for _, tagID := range manualTagIDs {
		_, err := tx.Exec(r.Context(), `
			INSERT INTO file_tags (file_id, tag_id, source, auto_rank, score)
			VALUES ($1, $2, 'MANUAL', NULL, NULL)
			ON CONFLICT (file_id, tag_id) DO UPDATE
			SET source = 'MANUAL',
				auto_rank = NULL,
				score = NULL,
				updated_at = NOW()
		`, fileID, tagID)
		if err != nil {
			return err
		}
	}

	if err := fh.syncLegacyTopTag(r, tx, fileID); err != nil {
		return err
	}

	if err := tx.Commit(r.Context()); err != nil {
		return err
	}

	tags, err := fh.loadFileTags(r, fileID)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, fileTagsResponse{FileID: fileID, Tags: tags})
}
