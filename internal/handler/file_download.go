package handler

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service"
)

func (fh *FileHandler) handleDownloadFile(w http.ResponseWriter, r *http.Request) error {
    user := getUserFromContext(r)
    if user == nil {
        http.Error(w, "Unauthorized", http.StatusUnauthorized)
        return nil
    }

    fileIDStr := r.URL.Query().Get("file_id")
    if fileIDStr == "" {
        http.Error(w, "Missing file_id", http.StatusBadRequest)
        return nil
    }

    fileID, err := strconv.Atoi(fileIDStr)
    if err != nil {
        http.Error(w, "Invalid file_id", http.StatusBadRequest)
        return nil
    }

    // Достаем запись из БД
    var f service.UserFile
    err = fh.UserService.DB.QueryRow(r.Context(),
        `SELECT id, user_id, file_name, file_path, file_size, file_type, created_at
         FROM files
         WHERE id = $1`, fileID,
    ).Scan(&f.ID, &f.UserID, &f.FileName, &f.FilePath, &f.FileSize, &f.FileType, &f.CreatedAt)
    if err != nil {
        http.Error(w, "File not found", http.StatusNotFound)
        return nil
    }

    // Проверим, что файл принадлежит текущему пользователю
    if f.UserID != user.ID {
        http.Error(w, "Forbidden", http.StatusForbidden)
        return nil
    }

    // Идём в Filer, скачиваем данные
    filerURL := fh.FilerURL + f.FilePath
    resp, err := http.Get(filerURL)
    if err != nil {
        http.Error(w, "Failed to download from filer", http.StatusInternalServerError)
        return nil
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        // Например, "404 Not Found" или что-то ещё
        bodyBytes, _ := io.ReadAll(resp.Body)
        http.Error(w, fmt.Sprintf("Filer error: %s", string(bodyBytes)), resp.StatusCode)
        return nil
    }

    // Устанавливаем заголовки, чтобы браузер скачивал (или открывал).
    // Content-Type: берем из базы (f.FileType) или из resp.Header.Get("Content-Type")
    // Content-Disposition: attachment; filename="..."
    w.Header().Set("Content-Type", f.FileType)
    downloadName := url.PathEscape(f.FileName) // экранировать пробелы и т.п.
    w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", downloadName))
    w.WriteHeader(http.StatusOK)

    // Проксируем тело
    _, err = io.Copy(w, resp.Body)
    if err != nil {
        // соединение могло обрываться, но уже ничего не поделаешь
        log.Printf("Error while copying file to response: %v", err)
    }

    return nil
}

