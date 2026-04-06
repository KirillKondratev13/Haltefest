package handler

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service"
)

// FileHandler будет хранить зависимости: UserService, FileService (если нужно), и URL Filer'а.
type FileHandler struct {
	UserService *service.UserService
	FileService *service.FileService
	FilerURL    string
	KafkaWriter KafkaWriter
}

type filesToParseEvent struct {
	FileID   int    `json:"file_id"`
	UserID   int    `json:"user_id"`
	S3Path   string `json:"s3_path"`
	MimeType string `json:"mime_type"`
}

const (
	mimeTypePDF  = "application/pdf"
	mimeTypeDOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)

func normalizeDetectedMime(fileName, detectedMime string) (string, string) {
	ext := strings.ToLower(filepath.Ext(fileName))
	switch ext {
	case ".docx":
		if detectedMime == "application/zip" || detectedMime == "application/octet-stream" {
			return mimeTypeDOCX, "extension_override_docx_from_zip_or_octet_stream"
		}
		if strings.Contains(detectedMime, "zip") {
			return mimeTypeDOCX, "extension_override_docx_from_zip_like_mime"
		}
		return mimeTypeDOCX, "extension_priority_docx"
	default:
		return detectedMime, "magic_bytes"
	}
}

func isSupportedMimeType(mimeType string) bool {
	return mimeType == mimeTypePDF ||
		mimeType == mimeTypeDOCX ||
		strings.HasPrefix(mimeType, "text/plain")
}

func previewHex(data []byte, n int) string {
	if n <= 0 || len(data) == 0 {
		return ""
	}
	if n > len(data) {
		n = len(data)
	}
	return hex.EncodeToString(data[:n])
}

func (fh *FileHandler) getUniqueFileName(ctx context.Context, userID int, originalName string) (string, error) {
	// Разделим имя файла на "название + расширение"
	// например, "photo.png" -> name="photo", ext=".png"
	ext := filepath.Ext(originalName)                        // ".png"
	baseName := originalName[0 : len(originalName)-len(ext)] // "photo"

	finalName := originalName
	suffix := 1

	for {
		// Проверим в БД, нет ли file_path = "/user_<userID>/finalName"
		pathToCheck := fmt.Sprintf("/user_%d/%s", userID, finalName)

		var exists bool
		err := fh.FileService.DB.QueryRow(ctx,
			`SELECT EXISTS(
                SELECT 1 FROM files
                WHERE user_id = $1
                  AND file_path = $2
            )`,
			userID,
			pathToCheck,
		).Scan(&exists)
		if err != nil {
			return "", err
		}

		if !exists {
			// значит свободно, используем finalName
			return finalName, nil
		}

		// иначе добавляем (1), (2) и т. д.
		finalName = fmt.Sprintf("%s(%d)%s", baseName, suffix, ext)
		suffix++
	}
}

func (fh *FileHandler) handleFileUpload(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	// ParseMultipartForm не нужен для стриминга, но нужен для FormFile
	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		http.Error(w, "Error parsing form data", http.StatusBadRequest)
		return nil
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "File not found in form", http.StatusBadRequest)
		return nil
	}
	defer file.Close()

	originalFileName := header.Filename
	headerMimeType := header.Header.Get("Content-Type")
	fileExt := strings.ToLower(filepath.Ext(originalFileName))

	// Определяем тип файла по magic bytes (первые 512 байт)
	buffer := make([]byte, 512)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		http.Error(w, "Error reading file header", http.StatusInternalServerError)
		return nil
	}
	detectedMimeType := http.DetectContentType(buffer[:n])
	fileType, mimeDecision := normalizeDetectedMime(originalFileName, detectedMimeType)

	slog.Info("file MIME detection",
		"original_file_name", originalFileName,
		"file_extension", fileExt,
		"multipart_header_mime", headerMimeType,
		"detected_mime_magic", detectedMimeType,
		"normalized_mime", fileType,
		"normalization_reason", mimeDecision,
		"header_bytes_read", n,
		"header_hex_preview", previewHex(buffer[:n], 16),
		"size_bytes", header.Size,
	)

	// Сбрасываем указатель в начало файла
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		http.Error(w, "Error resetting file pointer", http.StatusInternalServerError)
		return nil
	}

	// Получаем уникальное имя
	finalName, err := fh.getUniqueFileName(r.Context(), user.ID, originalFileName)
	if err != nil {
		http.Error(w, "Error checking unique filename", http.StatusInternalServerError)
		return nil
	}

	filerPath := fmt.Sprintf("/user_%d/%s", user.ID, finalName)
	uploadURL := fmt.Sprintf("%s%s", fh.FilerURL, filerPath)

	// Стримим файл напрямую в SeaweedFS
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		fw, err := mw.CreateFormFile("file", finalName)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		_, err = io.Copy(fw, file)
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		mw.Close()
	}()

	req, err := http.NewRequest("POST", uploadURL, pr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("filer upload error: %s", string(bodyBytes))
	}

	// Maximum file size for processing (25MB)
	const maxFileSize = 25 * 1024 * 1024
	fileSize := header.Size

	// Emit Kafka event only for supported document types (PDF, DOCX, TXT)
	isSupported := isSupportedMimeType(fileType)
	var status *string
	var failureCause *string
	processingStatus := "PROCESSING"
	errorStatus := "ERROR"

	if isSupported {
		status = &processingStatus
	}

	// Check file size for supported types
	decisionReason := "unsupported_mime"
	if isSupported {
		decisionReason = "supported_mime"
	}

	if isSupported && header.Size > maxFileSize {
		isSupported = false // Don't send to Kafka
		status = &errorStatus
		cause := "File too large (max 25MB)"
		failureCause = &cause
		decisionReason = "supported_mime_but_file_too_large"
		slog.Info("file too large for processing", "file_name", finalName, "size", header.Size)
	}

	slog.Info("file processing eligibility",
		"file_name", finalName,
		"file_extension", strings.ToLower(filepath.Ext(finalName)),
		"mime_type", fileType,
		"is_supported", isSupported,
		"decision_reason", decisionReason,
		"size_bytes", header.Size,
		"max_processing_size_bytes", maxFileSize,
	)

	now := time.Now()

	fileID, err := fh.FileService.SaveFile(r.Context(), service.UserFile{
		UserID:       user.ID,
		FileName:     finalName,
		FilePath:     filerPath,
		FileSize:     fileSize,
		FileType:     fileType,
		Status:       status,
		FailureCause: failureCause,
		CreatedAt:    now,
	})
	if err != nil {
		return err
	}

	if isSupported {
		payload := filesToParseEvent{
			FileID:   fileID,
			UserID:   user.ID,
			S3Path:   filerPath,
			MimeType: fileType,
		}
		if err := fh.KafkaWriter.WriteMessages(r.Context(), payload); err != nil {
			slog.Error("failed to emit kafka event", "error", err, "file_id", fileID, "mime_type", fileType, "decision_reason", decisionReason)
		} else {
			slog.Info("successfully emitted kafka event", "file_id", fileID, "mime_type", fileType, "decision_reason", decisionReason)
		}
	} else {
		slog.Info("skipping kafka event for unsupported file type", "file_id", fileID, "mime_type", fileType, "decision_reason", decisionReason)
	}

	// Для XHR — просто 200 OK (без редиректа)
	w.WriteHeader(http.StatusOK)
	return nil
}

// func (fh *FileHandler) handleFileUpload(w http.ResponseWriter, r *http.Request) error {
//     user := getUserFromContext(r)
//     if user == nil {
//         http.Error(w, "Unauthorized", http.StatusUnauthorized)
//         return nil
//     }

//     err := r.ParseMultipartForm(32 << 20)
//     if err != nil {
//         http.Error(w, "Error parsing form data", http.StatusBadRequest)
//         return nil
//     }

//     file, header, err := r.FormFile("file")
//     if err != nil {
//         http.Error(w, "File not found in form", http.StatusBadRequest)
//         return nil
//     }
//     defer file.Close()

//     // Считаем всё в память (пока для простоты)
//     fileBytes, err := io.ReadAll(file)
//     if err != nil {
//         http.Error(w, "Failed to read file content", http.StatusInternalServerError)
//         return nil
//     }

//     originalFileName := header.Filename
//     fileType := header.Header.Get("Content-Type")
//     if fileType == "" {
//         fileType = "application/octet-stream"
//     }

//     // Находим уникальное имя для этого пользователя
//     finalName, err := fh.getUniqueFileName(r.Context(), user.ID, originalFileName)
//     if err != nil {
//         http.Error(w, "Error checking unique filename", http.StatusInternalServerError)
//         return nil
//     }

//     // Формируем путь в Filer
//     // например, если finalName = "тест мад(1).PNG", тогда filerPath="/user_3/тест мад(1).PNG"
//     filerPath := fmt.Sprintf("/user_%d/%s", user.ID, finalName)

//     // Шлём файл в Filer
//     uploadURL := fmt.Sprintf("%s%s", fh.FilerURL, filerPath)

//     var buf bytes.Buffer
//     mw := multipart.NewWriter(&buf)
//     fw, err := mw.CreateFormFile("file", finalName)
//     if err != nil {
//         return err
//     }
//     _, err = fw.Write(fileBytes)
//     mw.Close()

//     req, err := http.NewRequest("POST", uploadURL, &buf)
//     if err != nil {
//         return err
//     }
//     req.Header.Set("Content-Type", mw.FormDataContentType())

//     resp, err := http.DefaultClient.Do(req)
//     if err != nil {
//         return err
//     }
//     defer resp.Body.Close()

//     if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
//         bodyBytes, _ := io.ReadAll(resp.Body)
//         return fmt.Errorf("filer upload error: %s", string(bodyBytes))
//     }

//     // Запишем метаданные
//     fileSize := int64(len(fileBytes))
//     now := time.Now()
//     _, err = fh.UserService.DB.Exec(r.Context(),
//         `INSERT INTO files (user_id, file_name, file_path, file_size, file_type, created_at)
//          VALUES ($1, $2, $3, $4, $5, $6)`,
//         user.ID, finalName, filerPath, fileSize, fileType, now)
//     if err != nil {
//         return err
//     }

//     http.Redirect(w, r, "/profile", http.StatusSeeOther)
//     return nil
// }
