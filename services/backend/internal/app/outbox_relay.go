package app

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

const (
	outboxStatusNew    = "NEW"
	outboxStatusSent   = "SENT"
	outboxStatusFailed = "FAILED"
)

type outboxEvent struct {
	ID        int64
	EventID   string
	EventType string
	Payload   []byte
	Attempts  int
}

type outboxRelay struct {
	db           *pgxpool.Pool
	kafkaBrokers string
	pollInterval time.Duration
	batchSize    int
	maxAttempts  int

	mu      sync.Mutex
	writers map[string]*kafka.Writer
}

func newOutboxRelay(db *pgxpool.Pool, kafkaBrokers string) *outboxRelay {
	return &outboxRelay{
		db:           db,
		kafkaBrokers: kafkaBrokers,
		pollInterval: 2 * time.Second,
		batchSize:    50,
		maxAttempts:  6,
		writers:      make(map[string]*kafka.Writer),
	}
}

func (r *outboxRelay) Run(ctx context.Context) {
	slog.Info("outbox relay started", "poll_interval", r.pollInterval.String(), "batch_size", r.batchSize)
	defer slog.Info("outbox relay stopped")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		processed, err := r.processBatch(ctx)
		if err != nil {
			slog.Error("outbox relay batch failed", "error", err.Error())
			if !r.sleepOrDone(ctx, r.pollInterval) {
				return
			}
			continue
		}

		if processed {
			continue
		}

		if !r.sleepOrDone(ctx, r.pollInterval) {
			return
		}
	}
}

func (r *outboxRelay) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for topic, writer := range r.writers {
		if err := writer.Close(); err != nil {
			slog.Error("failed to close outbox kafka writer", "topic", topic, "error", err.Error())
		}
	}
}

func (r *outboxRelay) processBatch(ctx context.Context) (bool, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, event_id::text, event_type, payload_json::text, attempts
		FROM outbox_events
		WHERE status = $1
		ORDER BY created_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, outboxStatusNew, r.batchSize)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var events []outboxEvent
	for rows.Next() {
		var event outboxEvent
		var payloadText string
		if err := rows.Scan(&event.ID, &event.EventID, &event.EventType, &payloadText, &event.Attempts); err != nil {
			return false, err
		}
		event.Payload = []byte(payloadText)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	if len(events) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}

	for _, event := range events {
		publishErr := r.publish(ctx, event)
		if publishErr != nil {
			nextAttempts := event.Attempts + 1
			nextStatus := outboxStatusNew
			if nextAttempts >= r.maxAttempts {
				nextStatus = outboxStatusFailed
			}

			_, err := tx.Exec(ctx, `
				UPDATE outbox_events
				SET status = $1, attempts = $2, last_error = $3
				WHERE id = $4
			`, nextStatus, nextAttempts, truncateError(publishErr.Error(), 1200), event.ID)
			if err != nil {
				return false, err
			}

			slog.Warn(
				"failed to publish outbox event",
				"event_id", event.EventID,
				"event_type", event.EventType,
				"attempt", nextAttempts,
				"status", nextStatus,
				"error", publishErr.Error(),
			)
			continue
		}

		_, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET status = $1, attempts = attempts + 1, last_error = NULL, sent_at = NOW()
			WHERE id = $2
		`, outboxStatusSent, event.ID)
		if err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (r *outboxRelay) publish(ctx context.Context, event outboxEvent) error {
	topic := strings.TrimSpace(event.EventType)
	if topic == "" {
		return fmt.Errorf("empty event_type for event_id=%s", event.EventID)
	}

	writer := r.writerForTopic(topic)
	return writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.EventID),
		Value: event.Payload,
		Time:  time.Now().UTC(),
	})
}

func (r *outboxRelay) writerForTopic(topic string) *kafka.Writer {
	r.mu.Lock()
	defer r.mu.Unlock()

	if writer, ok := r.writers[topic]; ok {
		return writer
	}

	writer := &kafka.Writer{
		Addr:     kafka.TCP(r.kafkaBrokers),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	r.writers[topic] = writer
	return writer
}

func (r *outboxRelay) sleepOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func truncateError(message string, maxLen int) string {
	if len(message) <= maxLen {
		return message
	}
	return message[:maxLen] + " ... [truncated]"
}
