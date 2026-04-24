package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type llmPreferences struct {
	SummaryProvider     string
	ChaptersProvider    string
	FlashcardsProvider  string
	ChatDefaultProvider string
}

type graphPreferences struct {
	ShowTagProbabilities     bool
	GalleryVisibility        string
	GallerySnapshotPath      *string
	GallerySnapshotUpdatedAt *time.Time
}

type llmPreferencesResponse struct {
	SummaryProvider          string     `json:"summary_provider"`
	ChaptersProvider         string     `json:"chapters_provider"`
	FlashcardsProvider       string     `json:"flashcards_provider"`
	ChatDefaultProvider      string     `json:"chat_default_provider"`
	ShowTagProbabilities     bool       `json:"show_tag_probabilities"`
	GalleryVisibility        string     `json:"gallery_visibility"`
	GallerySnapshotUpdatedAt *time.Time `json:"gallery_snapshot_updated_at,omitempty"`
}

type upsertPreferencesRequest struct {
	SummaryProvider      string `json:"summary_provider"`
	ChaptersProvider     string `json:"chapters_provider"`
	FlashcardsProvider   string `json:"flashcards_provider"`
	ChatDefaultProvider  string `json:"chat_default_provider"`
	ShowTagProbabilities *bool  `json:"show_tag_probabilities"`
	GalleryVisibility    string `json:"gallery_visibility"`
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

func defaultLLMPreferences() llmPreferences {
	return llmPreferences{
		SummaryProvider:     chatProviderLocal,
		ChaptersProvider:    chatProviderLocal,
		FlashcardsProvider:  chatProviderLocal,
		ChatDefaultProvider: chatProviderLocal,
	}
}

func defaultGraphPreferences() graphPreferences {
	return graphPreferences{
		ShowTagProbabilities: false,
		GalleryVisibility:    "private",
	}
}

func normalizeProviderOrDefault(value string, fallback string) string {
	provider, ok := normalizeLLMProvider(value)
	if ok {
		return provider
	}
	return fallback
}

func normalizeGalleryVisibilityOrDefault(value string, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "public":
		return "public"
	case "private":
		return "private"
	default:
		return fallback
	}
}

func parseRequiredProvider(value string, field string) (string, error) {
	provider, ok := normalizeLLMProvider(value)
	if !ok {
		return "", errors.New("invalid " + field)
	}
	return provider, nil
}

func providerForAnalysisType(prefs llmPreferences, analysisType string) string {
	switch strings.ToLower(strings.TrimSpace(analysisType)) {
	case "summary":
		return prefs.SummaryProvider
	case "chapters":
		return prefs.ChaptersProvider
	case "flashcards":
		return prefs.FlashcardsProvider
	default:
		return chatProviderLocal
	}
}

func toLLMPreferencesResponse(llmPrefs llmPreferences, graphPrefs graphPreferences) llmPreferencesResponse {
	return llmPreferencesResponse{
		SummaryProvider:          llmPrefs.SummaryProvider,
		ChaptersProvider:         llmPrefs.ChaptersProvider,
		FlashcardsProvider:       llmPrefs.FlashcardsProvider,
		ChatDefaultProvider:      llmPrefs.ChatDefaultProvider,
		ShowTagProbabilities:     graphPrefs.ShowTagProbabilities,
		GalleryVisibility:        graphPrefs.GalleryVisibility,
		GallerySnapshotUpdatedAt: graphPrefs.GallerySnapshotUpdatedAt,
	}
}

func (fh *FileHandler) loadUserLLMPreferences(ctx context.Context, db queryRower, userID int) (llmPreferences, error) {
	prefs := defaultLLMPreferences()
	var summaryProvider string
	var chaptersProvider string
	var flashcardsProvider string
	var chatDefaultProvider string

	err := db.QueryRow(ctx, `
		SELECT summary_provider, chapters_provider, flashcards_provider, chat_default_provider
		FROM user_llm_preferences
		WHERE user_id = $1
	`, userID).Scan(&summaryProvider, &chaptersProvider, &flashcardsProvider, &chatDefaultProvider)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return prefs, nil
		}
		return llmPreferences{}, err
	}

	prefs.SummaryProvider = normalizeProviderOrDefault(summaryProvider, prefs.SummaryProvider)
	prefs.ChaptersProvider = normalizeProviderOrDefault(chaptersProvider, prefs.ChaptersProvider)
	prefs.FlashcardsProvider = normalizeProviderOrDefault(flashcardsProvider, prefs.FlashcardsProvider)
	prefs.ChatDefaultProvider = normalizeProviderOrDefault(chatDefaultProvider, prefs.ChatDefaultProvider)

	return prefs, nil
}

func (fh *FileHandler) upsertUserLLMPreferences(ctx context.Context, userID int, prefs llmPreferences) error {
	now := time.Now().UTC()
	_, err := fh.FileService.DB.Exec(ctx, `
		INSERT INTO user_llm_preferences (
			user_id, summary_provider, chapters_provider, flashcards_provider, chat_default_provider, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $6)
		ON CONFLICT (user_id)
		DO UPDATE SET
			summary_provider = EXCLUDED.summary_provider,
			chapters_provider = EXCLUDED.chapters_provider,
			flashcards_provider = EXCLUDED.flashcards_provider,
			chat_default_provider = EXCLUDED.chat_default_provider,
			updated_at = EXCLUDED.updated_at
	`,
		userID,
		prefs.SummaryProvider,
		prefs.ChaptersProvider,
		prefs.FlashcardsProvider,
		prefs.ChatDefaultProvider,
		now,
	)
	return err
}

func (fh *FileHandler) loadUserGraphPreferences(ctx context.Context, db queryRower, userID int) (graphPreferences, error) {
	prefs := defaultGraphPreferences()
	var galleryVisibility string

	err := db.QueryRow(ctx, `
		SELECT show_tag_probabilities, gallery_visibility, gallery_snapshot_path, gallery_snapshot_updated_at
		FROM user_graph_preferences
		WHERE user_id = $1
	`, userID).Scan(&prefs.ShowTagProbabilities, &galleryVisibility, &prefs.GallerySnapshotPath, &prefs.GallerySnapshotUpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return prefs, nil
		}
		return graphPreferences{}, err
	}

	prefs.GalleryVisibility = normalizeGalleryVisibilityOrDefault(galleryVisibility, prefs.GalleryVisibility)
	return prefs, nil
}

func (fh *FileHandler) upsertUserGraphPreferences(ctx context.Context, userID int, prefs graphPreferences) error {
	now := time.Now().UTC()
	_, err := fh.FileService.DB.Exec(ctx, `
		INSERT INTO user_graph_preferences (
			user_id, show_tag_probabilities, gallery_visibility, gallery_snapshot_path, gallery_snapshot_updated_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $6)
		ON CONFLICT (user_id)
		DO UPDATE SET
			show_tag_probabilities = EXCLUDED.show_tag_probabilities,
			gallery_visibility = EXCLUDED.gallery_visibility,
			gallery_snapshot_path = EXCLUDED.gallery_snapshot_path,
			gallery_snapshot_updated_at = EXCLUDED.gallery_snapshot_updated_at,
			updated_at = EXCLUDED.updated_at
	`,
		userID,
		prefs.ShowTagProbabilities,
		prefs.GalleryVisibility,
		prefs.GallerySnapshotPath,
		prefs.GallerySnapshotUpdatedAt,
		now,
	)
	return err
}

func (fh *FileHandler) handleGetPreferences(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	llmPrefs, err := fh.loadUserLLMPreferences(r.Context(), fh.FileService.DB, user.ID)
	if err != nil {
		return err
	}

	graphPrefs, err := fh.loadUserGraphPreferences(r.Context(), fh.FileService.DB, user.ID)
	if err != nil {
		return err
	}

	return writeJSON(w, http.StatusOK, toLLMPreferencesResponse(llmPrefs, graphPrefs))
}

func (fh *FileHandler) handleUpsertPreferences(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	var request upsertPreferencesRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return nil
	}

	currentGraphPrefs, err := fh.loadUserGraphPreferences(r.Context(), fh.FileService.DB, user.ID)
	if err != nil {
		return err
	}

	summaryProvider, err := parseRequiredProvider(request.SummaryProvider, "summary_provider")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	chaptersProvider, err := parseRequiredProvider(request.ChaptersProvider, "chapters_provider")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	flashcardsProvider, err := parseRequiredProvider(request.FlashcardsProvider, "flashcards_provider")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	chatDefaultProvider, err := parseRequiredProvider(request.ChatDefaultProvider, "chat_default_provider")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	prefs := llmPreferences{
		SummaryProvider:     summaryProvider,
		ChaptersProvider:    chaptersProvider,
		FlashcardsProvider:  flashcardsProvider,
		ChatDefaultProvider: chatDefaultProvider,
	}
	if err := fh.upsertUserLLMPreferences(r.Context(), user.ID, prefs); err != nil {
		return err
	}

	nextGraphPrefs := currentGraphPrefs
	if request.ShowTagProbabilities != nil {
		nextGraphPrefs.ShowTagProbabilities = *request.ShowTagProbabilities
	}
	if strings.TrimSpace(request.GalleryVisibility) != "" {
		nextGraphPrefs.GalleryVisibility = normalizeGalleryVisibilityOrDefault(
			request.GalleryVisibility,
			nextGraphPrefs.GalleryVisibility,
		)
	}

	publishTransition :=
		currentGraphPrefs.GalleryVisibility != "public" &&
			nextGraphPrefs.GalleryVisibility == "public"
	if publishTransition {
		snapshotPath, snapshotUpdatedAt, err := fh.refreshUserGallerySnapshot(r.Context(), user.ID)
		if err != nil {
			return err
		}
		nextGraphPrefs.GallerySnapshotPath = &snapshotPath
		nextGraphPrefs.GallerySnapshotUpdatedAt = &snapshotUpdatedAt
	}

	if err := fh.upsertUserGraphPreferences(r.Context(), user.ID, nextGraphPrefs); err != nil {
		return err
	}

	return writeJSON(w, http.StatusOK, toLLMPreferencesResponse(prefs, nextGraphPrefs))
}

func (fh *FileHandler) handleRefreshGallerySnapshot(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	graphPrefs, err := fh.loadUserGraphPreferences(r.Context(), fh.FileService.DB, user.ID)
	if err != nil {
		return err
	}
	if graphPrefs.GalleryVisibility != "public" {
		http.Error(w, "Gallery must be public to refresh snapshot", http.StatusConflict)
		return nil
	}

	snapshotPath, snapshotUpdatedAt, err := fh.refreshUserGallerySnapshot(r.Context(), user.ID)
	if err != nil {
		return err
	}
	graphPrefs.GallerySnapshotPath = &snapshotPath
	graphPrefs.GallerySnapshotUpdatedAt = &snapshotUpdatedAt
	if err := fh.upsertUserGraphPreferences(r.Context(), user.ID, graphPrefs); err != nil {
		return err
	}

	llmPrefs, err := fh.loadUserLLMPreferences(r.Context(), fh.FileService.DB, user.ID)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, toLLMPreferencesResponse(llmPrefs, graphPrefs))
}
