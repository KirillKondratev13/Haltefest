package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

const snapshotSummaryCacheTTL = 12 * time.Hour

type gallerySnapshotTag struct {
	TagID       int64   `json:"tag_id"`
	DisplayName string  `json:"display_name"`
	FilesCount  int     `json:"files_count"`
	Coverage    float64 `json:"coverage"`
}

type gallerySnapshotPayload struct {
	Version     int                  `json:"version"`
	GeneratedAt string               `json:"generated_at"`
	OwnerUserID int                  `json:"owner_user_id"`
	Username    string               `json:"username"`
	FilesCount  int                  `json:"files_count"`
	TagsCount   int                  `json:"tags_count"`
	TopTags     []gallerySnapshotTag `json:"top_tags"`
}

type gallerySnapshotSummary struct {
	GeneratedAt string               `json:"generated_at"`
	FilesCount  int                  `json:"files_count"`
	TagsCount   int                  `json:"tags_count"`
	TopTags     []gallerySnapshotTag `json:"top_tags"`
}

func (fh *FileHandler) buildUserGraphSnapshot(ctx context.Context, userID int) (gallerySnapshotPayload, error) {
	var username string
	if err := fh.FileService.DB.QueryRow(ctx, `
		SELECT username
		FROM users
		WHERE id = $1
	`, userID).Scan(&username); err != nil {
		return gallerySnapshotPayload{}, err
	}

	filesCount, tagsCount, err := fh.loadUserGraphCountsFromContext(ctx, userID)
	if err != nil {
		return gallerySnapshotPayload{}, err
	}

	rows, err := fh.FileService.DB.Query(ctx, `
		SELECT t.id, t.display_name, COUNT(DISTINCT ft.file_id)::int AS files_count
		FROM file_tags ft
		JOIN tags t ON t.id = ft.tag_id
		JOIN files f ON f.id = ft.file_id
		WHERE f.user_id = $1
		GROUP BY t.id, t.display_name
		ORDER BY files_count DESC, t.display_name ASC
		LIMIT 20
	`, userID)
	if err != nil {
		return gallerySnapshotPayload{}, err
	}
	defer rows.Close()

	topTags := make([]gallerySnapshotTag, 0, 20)
	for rows.Next() {
		var tag gallerySnapshotTag
		if err := rows.Scan(&tag.TagID, &tag.DisplayName, &tag.FilesCount); err != nil {
			return gallerySnapshotPayload{}, err
		}
		if filesCount > 0 {
			tag.Coverage = float64(tag.FilesCount) / float64(filesCount)
		}
		topTags = append(topTags, tag)
	}
	if err := rows.Err(); err != nil {
		return gallerySnapshotPayload{}, err
	}

	return gallerySnapshotPayload{
		Version:     1,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		OwnerUserID: userID,
		Username:    username,
		FilesCount:  filesCount,
		TagsCount:   tagsCount,
		TopTags:     topTags,
	}, nil
}

func (fh *FileHandler) uploadGraphSnapshot(ctx context.Context, userID int, payload gallerySnapshotPayload) (string, time.Time, error) {
	nowUTC := time.Now().UTC()
	snapshotPath := fmt.Sprintf("/gallery_snapshots/user_%d/snapshot_%d.json", userID, nowUTC.Unix())

	content, err := json.Marshal(payload)
	if err != nil {
		return "", time.Time{}, err
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filepath.Base(snapshotPath))
	if err != nil {
		return "", time.Time{}, err
	}
	if _, err := part.Write(content); err != nil {
		return "", time.Time{}, err
	}
	if err := writer.Close(); err != nil {
		return "", time.Time{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fh.FilerURL+snapshotPath, &body)
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", time.Time{}, fmt.Errorf("snapshot upload failed: %s", string(bodyBytes))
	}

	return snapshotPath, nowUTC, nil
}

func (fh *FileHandler) refreshUserGallerySnapshot(ctx context.Context, userID int) (string, time.Time, error) {
	payload, err := fh.buildUserGraphSnapshot(ctx, userID)
	if err != nil {
		return "", time.Time{}, err
	}
	snapshotPath, snapshotUpdatedAt, err := fh.uploadGraphSnapshot(ctx, userID, payload)
	if err != nil {
		return "", time.Time{}, err
	}

	if fh.SnapshotCache != nil {
		summary := gallerySnapshotSummary{
			GeneratedAt: payload.GeneratedAt,
			FilesCount:  payload.FilesCount,
			TagsCount:   payload.TagsCount,
			TopTags:     payload.TopTags,
		}
		if encoded, err := json.Marshal(summary); err == nil {
			cacheKey := gallerySnapshotCacheKey(userID, snapshotUpdatedAt)
			_ = fh.SnapshotCache.Set(ctx, cacheKey, string(encoded), snapshotSummaryCacheTTL)
		}
	}

	return snapshotPath, snapshotUpdatedAt, nil
}

func gallerySnapshotCacheKey(ownerUserID int, snapshotUpdatedAt time.Time) string {
	return fmt.Sprintf("gallery:snapshot:v1:%d:%d", ownerUserID, snapshotUpdatedAt.UTC().Unix())
}

func (fh *FileHandler) loadGallerySnapshotSummary(ctx context.Context, ownerUserID int, snapshotPath string, snapshotUpdatedAt *time.Time) (*gallerySnapshotSummary, error) {
	path := strings.TrimSpace(snapshotPath)
	if path == "" {
		return nil, nil
	}

	if fh.SnapshotCache != nil && snapshotUpdatedAt != nil {
		cacheKey := gallerySnapshotCacheKey(ownerUserID, snapshotUpdatedAt.UTC())
		cachedValue, found, err := fh.SnapshotCache.Get(ctx, cacheKey)
		if err == nil && found && strings.TrimSpace(cachedValue) != "" {
			var summary gallerySnapshotSummary
			if decodeErr := json.Unmarshal([]byte(cachedValue), &summary); decodeErr == nil {
				return &summary, nil
			}
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fh.FilerURL+path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("snapshot read failed: %s", string(bodyBytes))
	}

	var payload gallerySnapshotPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	summary := &gallerySnapshotSummary{
		GeneratedAt: payload.GeneratedAt,
		FilesCount:  payload.FilesCount,
		TagsCount:   payload.TagsCount,
		TopTags:     payload.TopTags,
	}

	if fh.SnapshotCache != nil && snapshotUpdatedAt != nil {
		if encoded, err := json.Marshal(summary); err == nil {
			cacheKey := gallerySnapshotCacheKey(ownerUserID, snapshotUpdatedAt.UTC())
			_ = fh.SnapshotCache.Set(ctx, cacheKey, string(encoded), snapshotSummaryCacheTTL)
		}
	}

	return summary, nil
}
