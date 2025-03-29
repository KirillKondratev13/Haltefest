package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

type FileService struct {
    DB            *pgxpool.Pool
    MasterURL  string
    VolumeURL    string
}
func (s *FileService) UploadFile(ctx context.Context, userID int, file io.Reader, filename string) (*UserFile, error) {
    // 1. Читаем первые 512 байт для определения MIME-типа
    mimeBuffer := make([]byte, 512)
    n, err := file.Read(mimeBuffer)
    if err != nil && err != io.EOF {
        return nil, fmt.Errorf("failed to read file header: %w", err)
    }
    mimeType := http.DetectContentType(mimeBuffer[:n])

    // 2. Создаем pipe для потоковой передачи
    pr, pw := io.Pipe()
    defer pr.Close()

    // 3. Запрашиваем file_id у SeaweedFS
    assignResp, err := http.Get(s.MasterURL + "/dir/assign")
    if err != nil {
        return nil, fmt.Errorf("failed to get file ID: %w", err)
    }
    defer assignResp.Body.Close()

    var result struct {
        Fid string `json:"fid"`
        URL string `json:"url"`
    }
    if err := json.NewDecoder(assignResp.Body).Decode(&result); err != nil {
        return nil, fmt.Errorf("failed to parse assign response: %w", err)
    }

    // 4. Запускаем загрузку в отдельной горутине
    go func() {
        defer pw.Close()
        
        // Пишем первые 512 байт
        if _, err := pw.Write(mimeBuffer[:n]); err != nil {
            pw.CloseWithError(err)
            return
        }
        
        // Копируем остаток файла
        if _, err := io.Copy(pw, file); err != nil {
            pw.CloseWithError(err)
        }
    }()

    // 5. Создаем запрос с pipe reader
    uploadURL := "http://" + result.URL + "/" + result.Fid
    req, err := http.NewRequest("PUT", uploadURL, pr)
    if err != nil {
        return nil, fmt.Errorf("failed to create upload request: %w", err)
    }
    req.Header.Set("Content-Type", mimeType)

    // 6. Выполняем запрос
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, fmt.Errorf("upload failed: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusCreated {
        body, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("upload failed with status: %s, response: %s", resp.Status, string(body))
    }

    // 7. Получаем размер файла (через tee reader)
    sizeReader := &bytes.Buffer{}
    tee := io.TeeReader(io.MultiReader(bytes.NewReader(mimeBuffer[:n]), file), sizeReader)
    size, err := io.Copy(io.Discard, tee)
    if err != nil {
        return nil, fmt.Errorf("failed to get file size: %w", err)
    }

    // 8. Сохраняем метаданные
    var userFile UserFile
    err = s.DB.QueryRow(ctx,
        `INSERT INTO user_files 
        (user_id, seaweedfs_file_id, original_name, size, mime_type) 
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, created_at`,
        userID, result.Fid, filename, size, mimeType,
    ).Scan(&userFile.ID, &userFile.CreatedAt)

    return &userFile, err
}

// Загружает файл в SeaweedFS и сохраняет метаданные в PostgreSQL.
func (s *FileService) UploadFile1(ctx context.Context, userID int, file io.Reader, filename string) (*UserFile, error) {
    // 0. Подготовка: создаем MultiReader для повторного чтения файла
    var buf bytes.Buffer
    tee := io.TeeReader(file, &buf)
    combinedReader := io.MultiReader(&buf, file)

    // 1. Определяем MIME-тип
    mimeBuffer := make([]byte, 512)
    if _, err := tee.Read(mimeBuffer); err != nil && err != io.EOF {
        return nil, fmt.Errorf("failed to read file header: %w", err)
    }
    mimeType := http.DetectContentType(mimeBuffer)

    // 2. Определяем размер файла
    size, err := io.Copy(io.Discard, combinedReader)
    if err != nil {
        return nil, fmt.Errorf("failed to determine file size: %w", err)
    }

    // 3. Запрашиваем уникальный file_id у SeaweedFS
    assignResp, err := http.Get(s.MasterURL + "/dir/assign")
    if err != nil {
        return nil, fmt.Errorf("failed to get file ID: %w", err)
    }
    defer assignResp.Body.Close()

    var result struct {
        Fid string `json:"fid"`
        URL string `json:"url"`
    }
    if err := json.NewDecoder(assignResp.Body).Decode(&result); err != nil {
        return nil, fmt.Errorf("failed to parse assign response: %w", err)
    }

    // 4. Загружаем файл в SeaweedFS (читаем еще раз из буфера)
    uploadURL := "http://" + result.URL + "/" + result.Fid
    req, err := http.NewRequest("PUT", uploadURL, io.MultiReader(bytes.NewReader(mimeBuffer), &buf))
    if err != nil {
        return nil, fmt.Errorf("failed to create upload request: %w", err)
    }

    // Устанавливаем Content-Type
    req.Header.Set("Content-Type", mimeType)

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, fmt.Errorf("upload failed: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusCreated {
        body, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("upload failed with status: %s, response: %s", resp.Status, string(body))
    }

    // 5. Сохраняем метаданные в БД
    var userFile UserFile
    err = s.DB.QueryRow(ctx,
        `INSERT INTO user_files 
        (user_id, seaweedfs_file_id, original_name, size, mime_type) 
        VALUES ($1, $2, $3, $4, $5) 
        RETURNING id, created_at`,
        userID,
        result.Fid,
        filename,
        size,
        mimeType, // Используем определенный MIME-тип
    ).Scan(&userFile.ID, &userFile.CreatedAt)

    if err != nil {
        return nil, fmt.Errorf("failed to save file metadata: %w", err)
    }

    return &userFile, nil
}

func (s *FileService) GetUserFiles(ctx context.Context, userID int) ([]UserFile, error) {
    rows, err := s.DB.Query(ctx, 
        "SELECT id, seaweedfs_file_id, original_name, size, mime_type, created_at FROM user_files WHERE user_id = $1 ORDER BY created_at DESC",
        userID,
    )
    if err != nil {
        return nil, fmt.Errorf("db query: %w", err)
    }
    defer rows.Close()

    var files []UserFile
    for rows.Next() {
        var f UserFile
        if err := rows.Scan(&f.ID, &f.SeaweedFSFileID, &f.OriginalName, &f.Size, &f.MimeType, &f.CreatedAt); err != nil {
            return nil, fmt.Errorf("scan row: %w", err)
        }
        files = append(files, f)
    }

    return files, nil
}
