package service

import "time"

type UserFile struct {
    ID             int
    UserID         int
    SeaweedFSFileID string
    OriginalName   string
    Size           int64
    MimeType       string
    CreatedAt      time.Time
}