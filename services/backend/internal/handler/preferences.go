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

type llmPreferencesResponse struct {
	SummaryProvider     string `json:"summary_provider"`
	ChaptersProvider    string `json:"chapters_provider"`
	FlashcardsProvider  string `json:"flashcards_provider"`
	ChatDefaultProvider string `json:"chat_default_provider"`
}

type upsertPreferencesRequest struct {
	SummaryProvider     string `json:"summary_provider"`
	ChaptersProvider    string `json:"chapters_provider"`
	FlashcardsProvider  string `json:"flashcards_provider"`
	ChatDefaultProvider string `json:"chat_default_provider"`
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

func normalizeProviderOrDefault(value string, fallback string) string {
	provider, ok := normalizeLLMProvider(value)
	if ok {
		return provider
	}
	return fallback
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

func toLLMPreferencesResponse(prefs llmPreferences) llmPreferencesResponse {
	return llmPreferencesResponse{
		SummaryProvider:     prefs.SummaryProvider,
		ChaptersProvider:    prefs.ChaptersProvider,
		FlashcardsProvider:  prefs.FlashcardsProvider,
		ChatDefaultProvider: prefs.ChatDefaultProvider,
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

func (fh *FileHandler) handleGetPreferences(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	prefs, err := fh.loadUserLLMPreferences(r.Context(), fh.FileService.DB, user.ID)
	if err != nil {
		return err
	}

	return writeJSON(w, http.StatusOK, toLLMPreferencesResponse(prefs))
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

	return writeJSON(w, http.StatusOK, toLLMPreferencesResponse(prefs))
}
