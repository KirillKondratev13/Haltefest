package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"haltefest/parser/internal/config"
	"haltefest/parser/internal/parser"
	"haltefest/parser/internal/storage"

	"github.com/segmentio/kafka-go"
)

type FileMessage struct {
	FileID   int    `json:"file_id"`
	S3Path   string `json:"s3_path"`
	MimeType string `json:"mime_type"`
}

type ClassifyMessage struct {
	FileID   int    `json:"file_id"`
	CacheKey string `json:"cache_key"`
}

func main() {
	cfg := config.LoadConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Initialize Storage
	logger.Info("connecting to SeaweedFS", "filer_url", cfg.FilerURL)
	seaweed := storage.NewSeaweedClient(cfg.FilerURL)

	logger.Info("connecting to Redis", "redis_url", cfg.RedisURL)
	redis, err := storage.NewRedisClient(cfg.RedisURL)
	if err != nil {
		logger.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	logger.Info("redis connected")

	logger.Info("connecting to PostgreSQL", "conn_str", cfg.DBConnStr)
	db, err := storage.NewDBClient(cfg.DBConnStr)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	logger.Info("postgres connected")

	extractor := parser.NewExtractor()

	// Kafka Reader
	logger.Info("initializing Kafka reader",
		"brokers", cfg.KafkaBrokers,
		"topic", "files-to-parse",
		"group_id", "parser-service-group",
		"start_offset", "FirstOffset",
	)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{cfg.KafkaBrokers},
		Topic:       "files-to-parse",
		GroupID:     "parser-service-group",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	// Kafka Writer (topics are pre-created by kafka-init)
	logger.Info("initializing Kafka writer",
		"brokers", cfg.KafkaBrokers,
		"topic", "text-to-classify",
	)
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaBrokers),
		Topic:                  "text-to-classify",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: false,
	}
	defer writer.Close()

	logger.Info("Parser Service started", "brokers", cfg.KafkaBrokers, "workers", cfg.WorkerCount)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("shutting down...")
		cancel()
	}()

	// Worker Pool
	jobs := make(chan kafka.Message, cfg.WorkerCount)
	var wg sync.WaitGroup

	for i := 0; i < cfg.WorkerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for msg := range jobs {
				processMessage(ctx, logger, id, msg, seaweed, redis, db, extractor, writer)
				logger.Debug("committing offset", "worker_id", id, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
				if err := reader.CommitMessages(ctx, msg); err != nil {
					logger.Error("failed to commit offset", "worker_id", id, "error", err, "partition", msg.Partition, "offset", msg.Offset)
				} else {
					logger.Info("offset committed", "worker_id", id, "partition", msg.Partition, "offset", msg.Offset)
				}
			}
		}(i)
	}

	// Message Fetching Loop
	go func() {
		for {
			logger.Debug("fetching next message...")
			m, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error("failed to fetch message", "error", err)
				time.Sleep(1 * time.Second) // Avoid tight loop on error
				continue
			}
			logger.Info("received message from kafka", "topic", m.Topic, "partition", m.Partition, "offset", m.Offset)
			jobs <- m
			logger.Debug("message sent to worker pool")
		}
	}()

	<-ctx.Done()
	close(jobs)
	wg.Wait()
	logger.Info("Parser Service stopped")
}

func processMessage(
	ctx context.Context,
	logger *slog.Logger,
	workerID int,
	msg kafka.Message,
	seaweed *storage.SeaweedClient,
	redis *storage.RedisClient,
	db *storage.DBClient,
	extractor *parser.Extractor,
	writer *kafka.Writer,
) {
	var payload FileMessage
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		logger.Error("failed to unmarshal message", "worker_id", workerID, "error", err, "raw", string(msg.Value))
		return
	}

	start := time.Now()
	logger.Info("[STEP 0/5] worker processing file",
		"worker_id", workerID, "file_id", payload.FileID,
		"mime", payload.MimeType, "path", payload.S3Path,
	)

	// 1. Download
	logger.Debug("[STEP 1/5] downloading file from SeaweedFS", "file_id", payload.FileID, "path", payload.S3Path)
	data, err := seaweed.Download(payload.S3Path)
	if err != nil {
		reportError(ctx, logger, db, payload.FileID, fmt.Sprintf("failed to download: %v", err))
		return
	}
	logger.Info("[STEP 1/5] file downloaded", "file_id", payload.FileID, "size_bytes", len(data))

	// Limit check: 25MB
	if len(data) > 25*1024*1024 {
		reportError(ctx, logger, db, payload.FileID, "file size exceeds 25MB limit")
		return
	}

	// 2. Extract Text
	logger.Debug("[STEP 2/5] extracting text", "file_id", payload.FileID, "mime", payload.MimeType)
	text, err := extractor.Extract(data, payload.MimeType)
	if err != nil {
		reportError(ctx, logger, db, payload.FileID, fmt.Sprintf("parsing failed: %v", err))
		return
	}
	logger.Info("[STEP 2/5] text extracted", "file_id", payload.FileID, "text_length", len(text))

	// 2.1. Length Check (as per spec)
	if len(text) < 50 {
		reportError(ctx, logger, db, payload.FileID, "no text found or content too short (min 50 chars)")
		return
	}

	// 3. Language check
	logger.Debug("[STEP 3/5] detecting language", "file_id", payload.FileID)
	isEng, detectedLang := extractor.IsEnglish(text)
	logger.Info("[STEP 3/5] language detected", "file_id", payload.FileID, "is_english", isEng, "detected_lang", detectedLang)
	if !isEng {
		reportError(ctx, logger, db, payload.FileID, fmt.Sprintf("only English files are supported (detected: %s)", detectedLang))
		return
	}

	// 4. Save to Redis
	cacheKey := fmt.Sprintf("text:%d", payload.FileID)
	logger.Debug("[STEP 4/5] saving text to Redis", "file_id", payload.FileID, "cache_key", cacheKey, "text_length", len(text))
	if err := redis.Set(ctx, cacheKey, text, 24*time.Hour); err != nil {
		logger.Error("[STEP 4/5] failed to save to redis", "file_id", payload.FileID, "error", err)
		return
	}
	logger.Info("[STEP 4/5] text saved to Redis", "file_id", payload.FileID, "cache_key", cacheKey)

	// 5. Notify ML Service (with retry)
	logger.Debug("[STEP 5/5] sending classify event to Kafka", "file_id", payload.FileID, "topic", "text-to-classify")
	notifyMsg := ClassifyMessage{
		FileID:   payload.FileID,
		CacheKey: cacheKey,
	}
	notifyVal, _ := json.Marshal(notifyMsg)

	var writeErr error
	backoff := 500 * time.Millisecond
	for attempt := 1; attempt <= 3; attempt++ {
		writeErr = writer.WriteMessages(ctx, kafka.Message{Value: notifyVal})
		if writeErr == nil {
			logger.Info("[STEP 5/5] classify event sent",
				"file_id", payload.FileID, "topic", "text-to-classify", "attempt", attempt)
			break
		}
		logger.Warn("[STEP 5/5] failed to send classify event, retrying...",
			"file_id", payload.FileID, "attempt", attempt, "max_attempts", 3,
			"next_backoff", backoff*2, "error", writeErr)
		time.Sleep(backoff)
		backoff *= 2
	}
	if writeErr != nil {
		logger.Error("[STEP 5/5] failed to send classify event after all retries",
			"file_id", payload.FileID, "error", writeErr)
		return
	}

	elapsed := time.Since(start)
	logger.Info("file processed successfully",
		"file_id", payload.FileID, "worker_id", workerID, "duration", elapsed)
}

func reportError(ctx context.Context, logger *slog.Logger, db *storage.DBClient, fileID int, msg string) {
	logger.Warn("file processing error", "file_id", fileID, "message", msg)
	if err := db.ReportError(ctx, fileID, msg); err != nil {
		logger.Error("failed to report error to database", "file_id", fileID, "error", err)
	}
}
