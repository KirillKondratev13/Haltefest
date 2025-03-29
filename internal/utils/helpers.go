package utils

import "fmt"

func FormatFileSize(bytes int64) string {
    if bytes < 1024 {
        return fmt.Sprintf("%d B", bytes)
    } else if bytes < 1024*1024 {
        return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
    }
    return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
}