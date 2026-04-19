package handler

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

type startAnalysisRequest struct {
	AnalysisType string          `json:"analysis_type"`
	Params       json.RawMessage `json:"params"`
}

type startAnalysisResponse struct {
	JobID        int64  `json:"job_id"`
	Status       string `json:"status"`
	AnalysisType string `json:"analysis_type"`
	Provider     string `json:"provider"`
	FileID       int    `json:"file_id"`
	Reused       bool   `json:"reused"`
}

type analysisJobResponse struct {
	JobID        int64            `json:"job_id"`
	Status       string           `json:"status"`
	AnalysisType string           `json:"analysis_type"`
	Provider     string           `json:"provider"`
	FileID       int              `json:"file_id"`
	Error        *string          `json:"error"`
	Result       *json.RawMessage `json:"result"`
}

type latestAnalysisResponse struct {
	FileID       int             `json:"file_id"`
	AnalysisType string          `json:"analysis_type"`
	Provider     string          `json:"provider"`
	Status       string          `json:"status"`
	Result       json.RawMessage `json:"result"`
}

type analysisRequestedEvent struct {
	EventID      string `json:"event_id"`
	JobID        int64  `json:"job_id"`
	FileID       int    `json:"file_id"`
	UserID       int    `json:"user_id"`
	AnalysisType string `json:"analysis_type"`
	Provider     string `json:"provider"`
	RequestedAt  string `json:"requested_at"`
}

func (fh *FileHandler) handleStartAnalysis(w http.ResponseWriter, r *http.Request) error {
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

	var request startAnalysisRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return nil
	}

	analysisType, ok := normalizeAnalysisType(request.AnalysisType)
	if !ok {
		http.Error(w, "Invalid analysis_type", http.StatusBadRequest)
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

	prefs, err := fh.loadUserLLMPreferences(r.Context(), tx, user.ID)
	if err != nil {
		return err
	}
	provider := providerForAnalysisType(prefs, analysisType)

	var fileStatus *string
	err = tx.QueryRow(r.Context(), `
		SELECT status
		FROM files
		WHERE id = $1 AND user_id = $2
		FOR UPDATE
	`, fileID, user.ID).Scan(&fileStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "File not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	if fileStatus == nil || *fileStatus != "READY" {
		http.Error(w, "File is not ready for analysis", http.StatusConflict)
		return nil
	}

	var existingJobID int64
	var existingJobStatus string
	err = tx.QueryRow(r.Context(), `
		SELECT id, status
		FROM analysis_jobs
		WHERE user_id = $1
		  AND file_id = $2
		  AND analysis_type = $3
		  AND provider = $4
		  AND status IN ('QUEUED', 'PROCESSING')
		ORDER BY created_at DESC
		LIMIT 1
		FOR UPDATE
	`, user.ID, fileID, analysisType, provider).Scan(&existingJobID, &existingJobStatus)
	if err == nil {
		if err := tx.Commit(r.Context()); err != nil {
			return err
		}

		return writeJSON(w, http.StatusOK, startAnalysisResponse{
			JobID:        existingJobID,
			Status:       existingJobStatus,
			AnalysisType: analysisType,
			Provider:     provider,
			FileID:       fileID,
			Reused:       true,
		})
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	requestedAt := time.Now().UTC()

	var jobID int64
	var jobStatus string
	err = tx.QueryRow(r.Context(), `
		INSERT INTO analysis_jobs (
			file_id, user_id, analysis_type, provider, status, params_json, requested_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, 'QUEUED', $5::jsonb, $6, $6, $6)
		RETURNING id, status
	`, fileID, user.ID, analysisType, provider, string(paramsJSON), requestedAt).Scan(&jobID, &jobStatus)
	if err != nil {
		return err
	}

	eventID, err := newUUIDv4()
	if err != nil {
		return err
	}

	eventPayload, err := json.Marshal(analysisRequestedEvent{
		EventID:      eventID,
		JobID:        jobID,
		FileID:       fileID,
		UserID:       user.ID,
		AnalysisType: analysisType,
		Provider:     provider,
		RequestedAt:  requestedAt.Format(time.RFC3339),
	})
	if err != nil {
		return err
	}

	_, err = tx.Exec(r.Context(), `
		INSERT INTO outbox_events (
			event_id, aggregate_type, aggregate_id, event_type, payload_json, status, attempts
		)
		VALUES ($1::uuid, $2, $3, $4, $5::jsonb, 'NEW', 0)
	`, eventID, "analysis_job", jobID, "analysis-requested", string(eventPayload))
	if err != nil {
		return err
	}

	if err := tx.Commit(r.Context()); err != nil {
		return err
	}

	return writeJSON(w, http.StatusAccepted, startAnalysisResponse{
		JobID:        jobID,
		Status:       jobStatus,
		AnalysisType: analysisType,
		Provider:     provider,
		FileID:       fileID,
		Reused:       false,
	})
}

func (fh *FileHandler) handleGetAnalysisJob(w http.ResponseWriter, r *http.Request) error {
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

	var response analysisJobResponse
	err = fh.FileService.DB.QueryRow(r.Context(), `
		SELECT id, status, analysis_type, provider, file_id, error
		FROM analysis_jobs
		WHERE id = $1 AND user_id = $2
	`, jobID, user.ID).Scan(
		&response.JobID,
		&response.Status,
		&response.AnalysisType,
		&response.Provider,
		&response.FileID,
		&response.Error,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Job not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	if response.Status == "DONE" {
		var resultText string
		err = fh.FileService.DB.QueryRow(r.Context(), `
			SELECT result_json::text
			FROM analysis_results
			WHERE job_id = $1
		`, response.JobID).Scan(&resultText)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		} else {
			raw := json.RawMessage(resultText)
			response.Result = &raw
		}
	}

	return writeJSON(w, http.StatusOK, response)
}

func (fh *FileHandler) handleGetLatestAnalysis(w http.ResponseWriter, r *http.Request) error {
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

	analysisType, ok := normalizeAnalysisType(r.URL.Query().Get("type"))
	if !ok {
		http.Error(w, "Invalid type query param", http.StatusBadRequest)
		return nil
	}

	var fileExists int
	err = fh.FileService.DB.QueryRow(r.Context(), `
		SELECT 1
		FROM files
		WHERE id = $1 AND user_id = $2
	`, fileID, user.ID).Scan(&fileExists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "File not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	var resultText string
	var provider string
	err = fh.FileService.DB.QueryRow(r.Context(), `
		SELECT ar.result_json::text, aj.provider
		FROM analysis_jobs aj
		JOIN analysis_results ar ON ar.job_id = aj.id
		WHERE aj.user_id = $1
		  AND aj.file_id = $2
		  AND aj.analysis_type = $3
		  AND aj.status = 'DONE'
		ORDER BY aj.created_at DESC
		LIMIT 1
	`, user.ID, fileID, analysisType).Scan(&resultText, &provider)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Analysis result not found", http.StatusNotFound)
			return nil
		}
		return err
	}

	return writeJSON(w, http.StatusOK, latestAnalysisResponse{
		FileID:       fileID,
		AnalysisType: analysisType,
		Provider:     provider,
		Status:       "DONE",
		Result:       json.RawMessage(resultText),
	})
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(payload)
}

func normalizeAnalysisType(value string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "summary":
		return "summary", true
	case "chapters":
		return "chapters", true
	case "flashcards":
		return "flashcards", true
	default:
		return "", false
	}
}

func parsePathInt(value string, field string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, errors.New("missing " + field)
	}
	id, err := strconv.Atoi(value)
	if err != nil {
		return 0, errors.New("invalid " + field)
	}
	return id, nil
}

func parsePathInt64(value string, field string) (int64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, errors.New("missing " + field)
	}
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, errors.New("invalid " + field)
	}
	return id, nil
}

func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	hexEncoded := hex.EncodeToString(b[:])
	return hexEncoded[0:8] + "-" +
		hexEncoded[8:12] + "-" +
		hexEncoded[12:16] + "-" +
		hexEncoded[16:20] + "-" +
		hexEncoded[20:32], nil
}
