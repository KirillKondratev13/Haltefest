package handler

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"time"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service"
)

// FileHandler будет хранить зависимости: UserService, FileService (если нужно), и URL Filer'а.
type FileHandler struct {
    UserService *service.UserService
    // Дополнительно: чтобы не хардкодить url Filer’а, можно хранить его в config.
    FilerURL string // Например: "http://localhost:8888"
}

func (fh *FileHandler) getUniqueFileName(ctx context.Context, userID int, originalName string) (string, error) {
    // Разделим имя файла на "название + расширение"
    // например, "photo.png" -> name="photo", ext=".png"
    ext := filepath.Ext(originalName)            // ".png"
    baseName := originalName[0 : len(originalName)-len(ext)] // "photo"

    finalName := originalName
    suffix := 1

    for {
        // Проверим в БД, нет ли file_path = "/user_<userID>/finalName"
        pathToCheck := fmt.Sprintf("/user_%d/%s", userID, finalName)

        var exists bool
        err := fh.UserService.DB.QueryRow(ctx,
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

    // Определяем тип файла по magic bytes (первые 512 байт)
    buffer := make([]byte, 512)
    n, err := file.Read(buffer)
    if err != nil && err != io.EOF {
        http.Error(w, "Error reading file header", http.StatusInternalServerError)
        return nil
    }
    fileType := http.DetectContentType(buffer[:n])

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

    // Попробуем взять размер из header, если не получилось — ставим 0 (или реализуй подсчет через TeeReader)
    fileSize := header.Size
    fmt.Println()
    fmt.Println(filerPath, finalName)
    fmt.Println()
    now := time.Now()
    _, err = fh.UserService.DB.Exec(r.Context(),
        `INSERT INTO files (user_id, file_name, file_path, file_size, file_type, created_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        user.ID, finalName, filerPath, fileSize, fileType, now)
    if err != nil {
        return err
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

