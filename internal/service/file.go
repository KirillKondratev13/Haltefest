package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type FileService struct {
    DB            *pgxpool.Pool
    MasterURL  string
    VolumeURL    string
}

func (s *FileService) UploadFile(ctx context.Context, userID int, file io.Reader, filename string) (*UserFile, error) {
    // Читаем первые 512 байт, определяем MIME-тип
    mimeBuffer := make([]byte, 512)
    n, err := io.ReadFull(file, mimeBuffer)
    if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
        return nil, fmt.Errorf("failed to read file header: %w", err)
    }
    mimeType := http.DetectContentType(mimeBuffer[:n])

    // Получаем file_id от SeaweedFS
    
    assignURL := fmt.Sprintf("%s/assign", strings.TrimSuffix(s.MasterURL, "/"))
    fmt.Println("Requesting:", assignURL)

    assignResp, err := http.Get(assignURL)
    if err != nil {
        return nil, fmt.Errorf("failed to get file ID: %w", err)
    }
    defer assignResp.Body.Close()

    var result struct {
        Fid       string `json:"fid"`
        Url       string `json:"url"`
        PublicUrl string `json:"publicUrl"`
    }
    body, _ := io.ReadAll(assignResp.Body)
    fmt.Println("SeaweedFS assign response:", string(body))
    assignResp.Body = io.NopCloser(bytes.NewReader(body)) // Восстанавливаем тело ответа

    if err := json.NewDecoder(assignResp.Body).Decode(&result); err != nil {
        return nil, fmt.Errorf("failed to parse assign response: %w", err)
    }

    // Создаем pipe и bufer для потока
    pr, pw := io.Pipe()
    //bufWriter := bufio.NewWriter(pw)

    go func() {
        defer pw.Close()
        //defer bufWriter.Flush() может вызвать проблемы с CloseWithError
        
        // Сначала пишем MIME-буфер
        if _, err := /*bufWriter*/pw.Write(mimeBuffer[:n]); err != nil {
            pw.CloseWithError(err)
            return
        }
        // Затем оставшуюся часть файла
        if _, err := io.Copy(pw, file); err != nil {
            pw.CloseWithError(err)
        }
    }()

    // Отправляем файл в SeaweedFS
    uploadURL := fmt.Sprintf("http://%s/%s", result.Url, result.Fid)
    req, err := http.NewRequestWithContext(ctx, "PUT", uploadURL, pr)
    if err != nil {
        return nil, fmt.Errorf("failed to create upload request: %w", err)
    }
    req.Header.Set("Content-Type", mimeType)

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, fmt.Errorf("upload failed: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        body, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
    }

    // Записываем в БД
    var userFile UserFile
    err = s.DB.QueryRow(ctx,
        `INSERT INTO user_files (user_id, seaweedfs_file_id, original_name, size, mime_type) 
         VALUES ($1, $2, $3, $4, $5) RETURNING id, created_at`,
        userID, result.Fid, filename, int64(n), mimeType, // `int64(n)`, если мы знаем размер
    ).Scan(&userFile.ID, &userFile.CreatedAt)

    return &userFile, err
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
