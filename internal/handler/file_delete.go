package handler

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service"
)

func (fh *FileHandler) handleDeleteFile(w http.ResponseWriter, r *http.Request) error {
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

    if f.UserID != user.ID {
        http.Error(w, "Forbidden", http.StatusForbidden)
        return nil
    }

    // Отправляем DELETE в Filer
    // SeaweedFS Filer понимает DELETE /path
    req, err := http.NewRequest("DELETE", fh.FilerURL+f.FilePath, nil)
    if err != nil {
        return err
    }

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        http.Error(w, "Failed to delete file from filer", http.StatusInternalServerError)
        return nil
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
        bodyBytes, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("Filer delete error: %s", string(bodyBytes))
    }

    // Удаляем запись из БД
    _, err = fh.UserService.DB.Exec(r.Context(),
        "DELETE FROM files WHERE id = $1", fileID)
    if err != nil {
        return err
    }

    // Редирект обратно на профиль
    http.Redirect(w, r, "/profile", http.StatusSeeOther)
    return nil
}
