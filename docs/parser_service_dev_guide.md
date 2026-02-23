# Parser Service (Component B) - Development Guide

## Overview
This guide describes the architecture and logic for **Component B (Parser Service)**.
Designed for standalone development using Mock infrastructure and SeaweedFS.

## 1. System Interfaces

### Input: Kafka Consumer
- **Topic**: `files-to-parse`
- **Group ID**: `parser_service_group`
- **Payload Schema**:
  ```json
  {
    "file_id": 123,
    "s3_path": "/user_1/document.pdf",
    "mime_type": "application/pdf",
    "user_id": 1
  }
  ```
- **Security**: Internal network.

### Input: SeaweedFS (S3 Compatible)
- **Protocol**: HTTP
- **Action**: `GET {s3_path}`
- **Data**: Binary file stream.

### Output: DragonflyDB (Redis)
- **Protocol**: REDIS (`redis://...`)
- **Action**: `SETEX text:{file_id} 86400 "extracted_text"`
- **Data**: UTF-8 String.

### Output: Kafka Producer
- **Topic**: `text-to-classify`
- **Payload Schema**:
  ```json
  {
    "file_id": 123,
    "cache_key": "text:123",
    "request_id": "uuid-v4"
  }
  ```

### Output: PostgreSQL (Error Handling Only)
- **Action**: Update status to ERROR if parsing fails.
- **Query**:
  ```sql
  UPDATE files SET status = 'ERROR', error_msg = $1 WHERE id = $2
  ```

## 2. Service Logic (Go)

The service should be a worker listening to Kafka.

```go
func main() {
    // 1. Initialize Connections (SSL/TLS Mandatory)
    kafkaReader := kafka.NewReader(...)
    kafkaWriter := kafka.NewWriter(...)
    redisClient := redis.NewClient(...)
    s3Client := minio.New(...) // or aws-sdk-go-v2

    // 2. Processing Loop
    for {
        msg, err := kafkaReader.ReadMessage(ctx)
        payload := parseJSON(msg.Value)

        // Step 1: Download
        obj, err := s3Client.GetObject(..., payload.s3_path)
        if err != nil {
             handleError(payload.file_id, "S3 Download Failed")
             continue
        }

        // Step 2: Extract Text
        text, err := extractText(obj, payload.mime_type)
        // Libraries: 
        // - PDF: "github.com/pdfcpu/pdfcpu" or "rsc.io/pdf"
        // - DOCX: "baliance.com/gooxml" 
        
        // Step 3: Validate
        if !isEnglish(text) {
             handleError(payload.file_id, "Non-English text")
             continue
        }

        // Step 4: Save to Cache
        key := fmt.Sprintf("text:%d", payload.file_id)
        redisClient.Set(ctx, key, text, 24*time.Hour)

        // Step 5: Notify ML Service
        nextPayload := map[string]interface{}{
            "file_id": payload.file_id,
            "cache_key": key,
        }
        kafkaWriter.WriteMessages(..., nextPayload)
    }
}
```

## 3. Mock Data Setup (How to test manually)

### A. S3 Setup (SeaweedFS)
Upload a sample PDF manually via SeaweedFS Filer API or UI.
Assume path: `/user_1/test_doc.pdf`

### B. Kafka Trigger
Produce a test message to `files-to-parse`:
```bash
echo '{"file_id": 100, "s3_path": "/user_1/test_doc.pdf", "mime_type": "application/pdf"}' | kcat -P -b localhost:9092 -t files-to-parse
```

### C. Verify Output
1. Check Redis: `GET text:100` -> Should contain extracted text.
2. Check Kafka `text-to-classify`: Should receive a new message.

## 4. Environment Variables
- `KAFKA_BROKERS`: `localhost:9092`
- `REDIS_URL`: `redis://...`
- `S3_ENDPOINT`: `http://localhost:8333`
- `S3_ACCESS_KEY`: `...`
- `S3_SECRET_KEY`: `...`
