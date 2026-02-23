# Project Contract: Text Processing Pipeline

This document defines the strict interfaces between components in the Haltefest text processing pipeline.

## 1. Database Schema (Source of Truth)

Table: `files`

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `SERIAL` | Primary Key |
| `user_id` | `INT` | FK to users(id) |
| `file_name` | `TEXT` | Original filename |
| `file_path` | `TEXT` | Path in SeaweedFS (e.g., `/user_1/abc.pdf`) |
| `file_size` | `BIGINT` | Size in bytes |
| `file_type` | `TEXT` | MIME type |
| `status` | `VARCHAR` | `PENDING`, `PROCESSING`, `READY`, `ERROR` (NULL for skip) |
| `tag` | `VARCHAR` | Predicted label (NeoBERT) |
| `error_msg` | `TEXT` | Failure details |
| `created_at` | `TIMESTAMP` | Auto-set |

## 2. Kafka Event Schemas

### Topic: `files-to-parse`
Produced by: API (Go)
Consumed by: Parser Worker (Go)

```json
{
  "file_id": 123,
  "s3_path": "/user_1/doc.pdf",
  "mime_type": "application/pdf"
}
```

### Topic: `text-to-classify`
Produced by: Parser Worker (Go)
Consumed by: ML Service (Python)

```json
{
  "file_id": 123,
  "cache_key": "text:123"
}
```

## 3. Cache Storage (DragonflyDB)

- **Key Pattern**: `text:{file_id}`
- **Value**: Extracted UTF-8 text.
- **TTL**: 86400 seconds (24h).

## 4. Error Handling Policy

1. **Parser Worker Failures**:
   - If parsing fails (e.g., corrupted PDF), update `files` set `status='ERROR'`, `error_msg='...'`.
   - Log the error.
2. **ML Service Failures**:
   - If inference fails, update `files` set `status='ERROR'`, `error_msg='...'`.
   - Delete the cache key regardless.
3. **Kafka Retries**:
   - Use exponential backoff for DB/Redis connection issues.
   - For logic errors (invalid format), move to DLQ or mark as ERROR in DB.

## 5. Environment (Development)

- **Kafka**: `localhost:9092` (no SSL)
- **Redis**: `localhost:6379` (no SSL)
- **Postgres**: `localhost:5432` (no SSL)
- **SeaweedFS**: `http://localhost:8888` (filer)
