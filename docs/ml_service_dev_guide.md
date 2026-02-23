# ML Service (Component C) - Development Guide

## Overview
This guide describes the architecture and logic for **Component C (ML Service)**. 
Designed for standalone development using Mock infrastructure (Kafka, DragonflyDB, PostgreSQL).

## 1. System Interfaces

### Input: Kafka Consumer
- **Topic**: `text-to-classify`
- **Group ID**: `ml_service_group` (Important for scaling)
- **Payload Schema**:
  ```json
  {
    "file_id": 123,
    "cache_key": "text:123",
    "request_id": "uuid-v4"
  }
  ```
- **Security**: Internal network.

### Input: DragonflyDB (Redis)
- **Protocol**: REDIS (`redis://...`)
- **Action**: `GET {cache_key}`
- **Data**: Raw text string (UTF-8).

### Output: PostgreSQL
- **Table**: `files`
- **Action**: Update status and tag.
- **Query**:
  ```sql
  UPDATE files 
  SET tag = $1, status = 'READY', error_msg = NULL
  WHERE id = $2
  ```

## 2. Service Logic (Python/Go)

The service should be a long-running daemon.

```python
def main():
    # 1. Initialize Connections (SSL/TLS Mandatory)
    kafka_consumer = KafkaConsumer("text-to-classify", ...)
    redis_client = Redis(host=..., ssl=False)
    db_conn = psycopg2.connect(sslmode='disable')

    # 2. Processing Loop
    for msg in kafka_consumer:
        try:
            payload = json.loads(msg.value)
            file_id = payload['file_id']
            cache_key = payload['cache_key']

            # Step 1: Claim Check (Get Text)
            text = redis_client.get(cache_key)
            if not text:
                log.error(f"Text not found for key {cache_key}. TTL expired?")
                # Logic: Mark as Error in DB or Skip?
                mark_as_error(db_conn, file_id, "Text expired")
                continue

            # Step 2: Inference
            # Mocking Tip: For dev, just return random.choice(["Programming", "Biology"])
            tag = predict_neobert(text)

            # Step 3: Persistence
            update_db(db_conn, file_id, tag)

            # Step 4: Cleanup (Crucial!)
            redis_client.delete(cache_key)

            # Step 5: Commit Offset
            kafka_consumer.commit()

        except Exception as e:
            log.error(f"Failed to process file {file_id}: {e}")
            # Retry logic or Dead Letter Queue recommended here
```

## 3. Mock Data Setup (How to test manually)

Since you are developing this in isolation, you need to "seed" the infrastructure.

### A. Database Setup
```sql
INSERT INTO files (id, user_id, file_path, status) 
VALUES (100, 1, '/test/doc.pdf', 'PROCESSING');
```

### B. DragonflyDB Setup
Connect using `redis-cli` and set the mock text:
```bash
SETEX text:100 86400 "This is a sample text about Golang and system architecture..."
```

### C. Kafka Trigger
Produce a test message to trigger your service:
```bash
# Using kcat or similar tool
echo '{"file_id": 100, "cache_key": "text:100"}' | kcat -P -b localhost:9092 -t text-to-classify
```

## 4. Environment Variables
Ensure your service is configurable:
- `KAFKA_BROKERS`: `localhost:9092`
- `REDIS_URL`: `redis://:password@localhost:6379`
- `DATABASE_URL`: `postgres://user:pass@localhost:5432/db?sslmode=disable`
- `MODEL_PATH`: `./neobert_v1`
