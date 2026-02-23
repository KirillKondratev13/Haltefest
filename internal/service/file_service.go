package service

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type UserFile struct {
    ID        int
    UserID    int
    FileName  string
    FilePath  string
    FileSize  int64
    FileType  string
    Status    *string // PENDING, PROCESSING, READY, ERROR
    Tag       *string
    ErrorMsg  *string
    CreatedAt time.Time
    DownloadURL string
    DeleteURL  string
}

type FileService struct {
    DB *pgxpool.Pool
}

func (s *FileService) SaveFile(ctx context.Context, f UserFile) error {
    _, err := s.DB.Exec(ctx, `
        INSERT INTO files (user_id, file_name, file_path, file_size, file_type, status, tag, error_msg, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `, f.UserID, f.FileName, f.FilePath, f.FileSize, f.FileType, f.Status, f.Tag, f.ErrorMsg, f.CreatedAt)
    return err
}

func (s *FileService) GetUserFiles(ctx context.Context, userID int) ([]UserFile, error) {
    rows, err := s.DB.Query(ctx, `
        SELECT id, user_id, file_name, file_path, file_size, file_type, status, tag, error_msg, created_at
        FROM files
        WHERE user_id = $1
        ORDER BY created_at DESC
    `, userID)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var result []UserFile
    for rows.Next() {
        var f UserFile
        err := rows.Scan(&f.ID, &f.UserID, &f.FileName, &f.FilePath, &f.FileSize, &f.FileType, &f.Status, &f.Tag, &f.ErrorMsg, &f.CreatedAt)
        if err != nil {
            return nil, err
        }
        result = append(result, f)
    }
    return result, rows.Err()
}

func (s *FileService) DeleteFile(ctx context.Context, fileID, userID int) error {
    _, err := s.DB.Exec(ctx, "DELETE FROM files WHERE id = $1 AND user_id = $2", fileID, userID)
    return err
}

func (s *FileService) GetFileByID(ctx context.Context, fileID, userID int) (*UserFile, error) {
    var f UserFile
    err := s.DB.QueryRow(ctx, `
        SELECT id, user_id, file_name, file_path, file_size, file_type, status, tag, error_msg, created_at
        FROM files
        WHERE id = $1 AND user_id = $2
    `, fileID, userID).Scan(&f.ID, &f.UserID, &f.FileName, &f.FilePath, &f.FileSize, &f.FileType, &f.Status, &f.Tag, &f.ErrorMsg, &f.CreatedAt)
    if err != nil {
        return nil, err
    }
    return &f, nil
}
