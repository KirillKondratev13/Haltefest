package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

const (
	chatScopeSingleDoc = "single-doc"
	chatScopeMultiDoc  = "multi-doc"
	chatScopeAllDocs   = "all-docs"

	chatProviderLocal    = "local"
	chatProviderGigachat = "gigachat"

	chatJobStatusQueued = "QUEUED"
	chatRoutingAuto     = "AUTO"
)

type createChatThreadRequest struct {
	Scope           string `json:"scope"`
	SelectedFileIDs []int  `json:"selected_file_ids"`
	Title           string `json:"title"`
	Provider        string `json:"provider"`
}

type chatThreadResponse struct {
	ThreadID        int64     `json:"thread_id"`
	Title           string    `json:"title"`
	Scope           string    `json:"scope"`
	Provider        string    `json:"provider"`
	SelectedFileIDs []int     `json:"selected_file_ids"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type chatThreadsListResponse struct {
	Threads []chatThreadResponse `json:"threads"`
}

type createChatMessageRequest struct {
	Content  string          `json:"content"`
	Provider string          `json:"provider"`
	Params   json.RawMessage `json:"params"`
}

type createChatMessageResponse struct {
	ChatID            int64     `json:"chat_id"`
	MessageID         int64     `json:"message_id"`
	JobID             int64     `json:"job_id"`
	Status            string    `json:"status"`
	Provider          string    `json:"provider"`
	RoutingMode       string    `json:"routing_mode"`
	ScopeMode         string    `json:"scope_mode"`
	SelectedFileIDs   []int     `json:"selected_file_ids"`
	RequestedAt       time.Time `json:"requested_at"`
	QuestionCreatedAt time.Time `json:"question_created_at"`
}

type chatRequestedEvent struct {
	EventID           string `json:"event_id"`
	JobID             int64  `json:"job_id"`
	ChatID            int64  `json:"chat_id"`
	QuestionMessageID int64  `json:"question_message_id"`
	UserID            int    `json:"user_id"`
	Provider          string `json:"provider"`
	ScopeMode         string `json:"scope_mode"`
	SelectedFileIDs   []int  `json:"selected_file_ids"`
	RequestedAt       string `json:"requested_at"`
}

type chatMessageItem struct {
	MessageID int64     `json:"message_id"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type chatMessagesResponse struct {
	ThreadID  int64             `json:"thread_id"`
	ScopeMode string            `json:"scope_mode"`
	Messages  []chatMessageItem `json:"messages"`
}

type chatJobResponse struct {
	JobID             int64      `json:"job_id"`
	ChatID            int64      `json:"chat_id"`
	QuestionMessageID int64      `json:"question_message_id"`
	AssistantMessage  *string    `json:"assistant_message"`
	Status            string     `json:"status"`
	Provider          string     `json:"provider"`
	RoutingMode       string     `json:"routing_mode"`
	ScopeMode         string     `json:"scope_mode"`
	Error             *string    `json:"error"`
	RequestedAt       time.Time  `json:"requested_at"`
	StartedAt         *time.Time `json:"started_at"`
	FinishedAt        *time.Time `json:"finished_at"`
}

func (fh *FileHandler) handleCreateChatThread(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	var request createChatThreadRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return nil
	}

	scopeMode, ok := normalizeChatScope(request.Scope)
	if !ok {
		http.Error(w, "Invalid scope", http.StatusBadRequest)
		return nil
	}

	selectedFileIDs := sanitizeFileIDs(request.SelectedFileIDs)
	if err := validateScopeSelection(scopeMode, selectedFileIDs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	provider := strings.TrimSpace(request.Provider)
	if provider == "" {
		provider = chatProviderLocal
	}
	provider, ok = normalizeLLMProvider(provider)
	if !ok {
		http.Error(w, "Invalid provider", http.StatusBadRequest)
		return nil
	}

	selectedFileIDsJSON, err := json.Marshal(selectedFileIDs)
	if err != nil {
		return err
	}

	now := time.Now().UTC()

	tx, err := fh.FileService.DB.Begin(r.Context())
	if err != nil {
		return err
	}
	defer tx.Rollback(r.Context())

	if err := ensureUserOwnsSelectedFiles(r.Context(), tx, user.ID, selectedFileIDs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	title := strings.TrimSpace(request.Title)
	var response chatThreadResponse
	err = tx.QueryRow(r.Context(), `
		INSERT INTO chat_threads (
			user_id, title, scope_mode, selected_file_ids_json, default_provider, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, $6)
		RETURNING id, title, scope_mode, default_provider, selected_file_ids_json, created_at, updated_at
	`, user.ID, title, scopeMode, string(selectedFileIDsJSON), provider, now).Scan(
		&response.ThreadID,
		&response.Title,
		&response.Scope,
		&response.Provider,
		&selectedFileIDsJSON,
		&response.CreatedAt,
		&response.UpdatedAt,
	)
	if err != nil {
		return err
	}

	response.SelectedFileIDs = parseSelectedFileIDsJSON(selectedFileIDsJSON)

	if err := tx.Commit(r.Context()); err != nil {
		return err
	}

	return writeJSON(w, http.StatusCreated, response)
}

func (fh *FileHandler) handleListChatThreads(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	rows, err := fh.FileService.DB.Query(r.Context(), `
		SELECT id, title, scope_mode, default_provider, selected_file_ids_json, created_at, updated_at
		FROM chat_threads
		WHERE user_id = $1
		ORDER BY updated_at DESC, id DESC
	`, user.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	response := chatThreadsListResponse{Threads: make([]chatThreadResponse, 0)}
	for rows.Next() {
		var item chatThreadResponse
		var selectedIDsRaw []byte
		if err := rows.Scan(
			&item.ThreadID,
			&item.Title,
			&item.Scope,
			&item.Provider,
			&selectedIDsRaw,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return err
		}
		item.SelectedFileIDs = parseSelectedFileIDsJSON(selectedIDsRaw)
		response.Threads = append(response.Threads, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return writeJSON(w, http.StatusOK, response)
}

func (fh *FileHandler) handleGetChatMessages(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	threadID, err := parsePathInt64(chi.URLParam(r, "thread_id"), "thread_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	var scopeMode string
	err = fh.FileService.DB.QueryRow(r.Context(), `
		SELECT scope_mode
		FROM chat_threads
		WHERE id = $1 AND user_id = $2
	`, threadID, user.ID).Scan(&scopeMode)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Chat thread not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	rows, err := fh.FileService.DB.Query(r.Context(), `
		SELECT id, role, content, created_at
		FROM chat_messages
		WHERE chat_id = $1 AND user_id = $2
		ORDER BY created_at ASC, id ASC
	`, threadID, user.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	response := chatMessagesResponse{
		ThreadID:  threadID,
		ScopeMode: scopeMode,
		Messages:  make([]chatMessageItem, 0),
	}

	for rows.Next() {
		var message chatMessageItem
		if err := rows.Scan(&message.MessageID, &message.Role, &message.Content, &message.CreatedAt); err != nil {
			return err
		}
		response.Messages = append(response.Messages, message)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return writeJSON(w, http.StatusOK, response)
}

func (fh *FileHandler) handleDeleteChatThread(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	threadID, err := parsePathInt64(chi.URLParam(r, "thread_id"), "thread_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	result, err := fh.FileService.DB.Exec(r.Context(), `
		DELETE FROM chat_threads
		WHERE id = $1 AND user_id = $2
	`, threadID, user.ID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		http.Error(w, "Chat thread not found", http.StatusNotFound)
		return nil
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (fh *FileHandler) handleCreateChatMessage(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	threadID, err := parsePathInt64(chi.URLParam(r, "thread_id"), "thread_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	var request createChatMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return nil
	}

	content := strings.TrimSpace(request.Content)
	if content == "" {
		http.Error(w, "Content is required", http.StatusBadRequest)
		return nil
	}

	paramsJSON := []byte(`{}`)
	if len(request.Params) > 0 {
		paramsJSON = request.Params
	}

	tx, err := fh.FileService.DB.Begin(r.Context())
	if err != nil {
		return err
	}
	defer tx.Rollback(r.Context())

	var scopeMode string
	var selectedIDsRaw []byte
	var defaultProvider string
	err = tx.QueryRow(r.Context(), `
		SELECT scope_mode, selected_file_ids_json, default_provider
		FROM chat_threads
		WHERE id = $1 AND user_id = $2
		FOR UPDATE
	`, threadID, user.ID).Scan(&scopeMode, &selectedIDsRaw, &defaultProvider)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Chat thread not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	provider := strings.TrimSpace(request.Provider)
	if provider == "" {
		provider = defaultProvider
	}
	provider, ok := normalizeLLMProvider(provider)
	if !ok {
		http.Error(w, "Invalid provider", http.StatusBadRequest)
		return nil
	}

	selectedFileIDs := parseSelectedFileIDsJSON(selectedIDsRaw)
	if err := validateScopeSelection(scopeMode, selectedFileIDs); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return nil
	}

	now := time.Now().UTC()

	var questionMessageID int64
	var questionCreatedAt time.Time
	err = tx.QueryRow(r.Context(), `
		INSERT INTO chat_messages (chat_id, user_id, role, content, metadata_json, created_at)
		VALUES ($1, $2, 'user', $3, '{}'::jsonb, $4)
		RETURNING id, created_at
	`, threadID, user.ID, content, now).Scan(&questionMessageID, &questionCreatedAt)
	if err != nil {
		return err
	}

	var jobID int64
	var jobStatus string
	var routingMode string
	var requestedAt time.Time
	err = tx.QueryRow(r.Context(), `
		INSERT INTO chat_jobs (
			chat_id, user_id, question_message_id, status, provider, routing_mode, scope_mode,
			selected_file_ids_json, params_json, requested_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $10, $10)
		RETURNING id, status, routing_mode, requested_at
	`, threadID, user.ID, questionMessageID, chatJobStatusQueued, provider, chatRoutingAuto, scopeMode, string(selectedIDsRaw), string(paramsJSON), now).Scan(
		&jobID,
		&jobStatus,
		&routingMode,
		&requestedAt,
	)
	if err != nil {
		return err
	}

	eventID, err := newUUIDv4()
	if err != nil {
		return err
	}

	eventPayload, err := json.Marshal(chatRequestedEvent{
		EventID:           eventID,
		JobID:             jobID,
		ChatID:            threadID,
		QuestionMessageID: questionMessageID,
		UserID:            user.ID,
		Provider:          provider,
		ScopeMode:         scopeMode,
		SelectedFileIDs:   selectedFileIDs,
		RequestedAt:       requestedAt.Format(time.RFC3339),
	})
	if err != nil {
		return err
	}

	_, err = tx.Exec(r.Context(), `
		INSERT INTO outbox_events (
			event_id, aggregate_type, aggregate_id, event_type, payload_json, status, attempts
		)
		VALUES ($1::uuid, $2, $3, $4, $5::jsonb, 'NEW', 0)
	`, eventID, "chat_job", jobID, "chat-requested", string(eventPayload))
	if err != nil {
		return err
	}

	_, err = tx.Exec(r.Context(), `
		UPDATE chat_threads
		SET updated_at = $1
		WHERE id = $2
	`, requestedAt, threadID)
	if err != nil {
		return err
	}

	if err := tx.Commit(r.Context()); err != nil {
		return err
	}

	return writeJSON(w, http.StatusAccepted, createChatMessageResponse{
		ChatID:            threadID,
		MessageID:         questionMessageID,
		JobID:             jobID,
		Status:            jobStatus,
		Provider:          provider,
		RoutingMode:       routingMode,
		ScopeMode:         scopeMode,
		SelectedFileIDs:   selectedFileIDs,
		RequestedAt:       requestedAt,
		QuestionCreatedAt: questionCreatedAt,
	})
}

func (fh *FileHandler) handleGetChatJob(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	jobID, err := parsePathInt64(chi.URLParam(r, "job_id"), "job_id")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	var response chatJobResponse
	var assistantMessageID *int64
	err = fh.FileService.DB.QueryRow(r.Context(), `
		SELECT
			id,
			chat_id,
			question_message_id,
			assistant_message_id,
			status,
			provider,
			routing_mode,
			scope_mode,
			error,
			requested_at,
			started_at,
			finished_at
		FROM chat_jobs
		WHERE id = $1 AND user_id = $2
	`, jobID, user.ID).Scan(
		&response.JobID,
		&response.ChatID,
		&response.QuestionMessageID,
		&assistantMessageID,
		&response.Status,
		&response.Provider,
		&response.RoutingMode,
		&response.ScopeMode,
		&response.Error,
		&response.RequestedAt,
		&response.StartedAt,
		&response.FinishedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Chat job not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	if assistantMessageID != nil {
		var assistantText string
		err = fh.FileService.DB.QueryRow(r.Context(), `
			SELECT content
			FROM chat_messages
			WHERE id = $1 AND user_id = $2
		`, *assistantMessageID, user.ID).Scan(&assistantText)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		} else {
			response.AssistantMessage = &assistantText
		}
	}

	return writeJSON(w, http.StatusOK, response)
}

func normalizeChatScope(value string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "single-doc", "single_doc", "single":
		return chatScopeSingleDoc, true
	case "multi-doc", "multi_doc", "multi":
		return chatScopeMultiDoc, true
	case "all-docs", "all_docs", "all":
		return chatScopeAllDocs, true
	default:
		return "", false
	}
}

func normalizeLLMProvider(value string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case chatProviderLocal:
		return chatProviderLocal, true
	case chatProviderGigachat:
		return chatProviderGigachat, true
	default:
		return "", false
	}
}

func sanitizeFileIDs(input []int) []int {
	if len(input) == 0 {
		return []int{}
	}

	seen := make(map[int]struct{}, len(input))
	result := make([]int, 0, len(input))
	for _, id := range input {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func validateScopeSelection(scopeMode string, selectedFileIDs []int) error {
	switch scopeMode {
	case chatScopeSingleDoc:
		if len(selectedFileIDs) != 1 {
			return errors.New("single-doc scope requires exactly one selected_file_id")
		}
	case chatScopeMultiDoc:
		if len(selectedFileIDs) < 2 {
			return errors.New("multi-doc scope requires at least two selected_file_ids")
		}
	case chatScopeAllDocs:
		if len(selectedFileIDs) != 0 {
			return errors.New("all-docs scope does not accept selected_file_ids")
		}
	default:
		return errors.New("invalid scope mode")
	}
	return nil
}

func ensureUserOwnsSelectedFiles(ctx context.Context, tx pgx.Tx, userID int, selectedFileIDs []int) error {
	if len(selectedFileIDs) == 0 {
		return nil
	}

	ids32 := make([]int32, 0, len(selectedFileIDs))
	for _, id := range selectedFileIDs {
		if id > int(^uint32(0)>>1) {
			return fmt.Errorf("invalid file id: %d", id)
		}
		ids32 = append(ids32, int32(id))
	}

	var ownedCount int
	err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM files
		WHERE user_id = $1 AND id = ANY($2)
	`, userID, ids32).Scan(&ownedCount)
	if err != nil {
		return err
	}
	if ownedCount != len(selectedFileIDs) {
		return errors.New("one or more selected files are not found or not accessible")
	}

	return nil
}

func parseSelectedFileIDsJSON(raw []byte) []int {
	if len(raw) == 0 {
		return []int{}
	}
	var ids []int
	if err := json.Unmarshal(raw, &ids); err != nil {
		return []int{}
	}
	return sanitizeFileIDs(ids)
}
