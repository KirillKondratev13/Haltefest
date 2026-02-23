package handler

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

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

    f, err := fh.FileService.GetFileByID(r.Context(), fileID, user.ID)
    if err != nil {
        http.Error(w, "Файл не найден или доступ запрещен", http.StatusNotFound)
        return nil
    }

    // Отправляем DELETE в Filer
    req, err := http.NewRequest("DELETE", fh.FilerURL+f.FilePath, nil)
    if err != nil {
        return err
    }

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        http.Error(w, "Не удалось удалить файл из хранилища", http.StatusInternalServerError)
        return nil
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
        bodyBytes, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("Filer delete error: %s", string(bodyBytes))
    }

    // Удаляем запись из БД
    err = fh.FileService.DeleteFile(r.Context(), fileID, user.ID)
    if err != nil {
        return err
    }

    // Редирект обратно на профиль
    http.Redirect(w, r, "/profile", http.StatusSeeOther)
    return nil
}
