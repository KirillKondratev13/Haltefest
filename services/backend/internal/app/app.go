package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/cache"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/config"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/handler"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service" // Импортируем service
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type kafkaProducer struct {
	writer *kafka.Writer
}

func (kp *kafkaProducer) WriteMessages(ctx context.Context, msgs ...interface{}) error {
	kmsgs := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		val, err := json.Marshal(m)
		if err != nil {
			return err
		}
		kmsgs[i] = kafka.Message{
			Value: val,
		}
	}
	return kp.writer.WriteMessages(ctx, kmsgs...)
}

func Run(ctx context.Context) error {
	cfg := config.NewConfig()

	// Подключение к базе данных
	dbpool, err := pgxpool.New(ctx, cfg.DBConnStr)
	if err != nil {
		slog.Error("Unable to connect to database", slog.String("error", err.Error()))
		return err
	}
	defer dbpool.Close()

	// Запуск миграций
	if err := runMigrations(ctx, dbpool); err != nil {
		slog.Error("Failed to run migrations", slog.String("error", err.Error()))
		return err
	}

	// Создаем сервисы
	userService := &service.UserService{DB: dbpool}
	fileService := &service.FileService{DB: dbpool}

	// Инициализация Kafka
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers),
		Topic:    "files-to-parse",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	kp := &kafkaProducer{writer: kafkaWriter}

	var snapshotCache handler.SnapshotCache
	if strings.TrimSpace(cfg.DragonflyAddr) != "" {
		snapshotCache = cache.NewDragonflyClient(cfg.DragonflyAddr, cfg.DragonflyPassword)
		slog.Info("dragonfly snapshot cache enabled", slog.String("addr", cfg.DragonflyAddr))
	}

	relay := newOutboxRelay(dbpool, cfg.KafkaBrokers)
	defer relay.Close()
	go relay.Run(ctx)

	r := chi.NewRouter()
	handler.RegisterRoutes(r, handler.Dependencies{
		AssetsFS:      http.Dir(cfg.AssetsDir),
		UserService:   userService,
		FileService:   fileService,
		Config:        cfg,
		KafkaWriter:   kp,
		SnapshotCache: snapshotCache,
	})

	s := http.Server{
		Addr:    cfg.ServerAddr,
		Handler: r,
	}

	go func() {
		<-ctx.Done()
		slog.Info("shutting down server")
		// Используем новый контекст с таймаутом для завершения работы
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Shutdown(shutdownCtx)
	}()

	slog.Info("starting server", slog.String("addr", cfg.ServerAddr))
	if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

func runMigrations(ctx context.Context, db *pgxpool.Pool) error {
	slog.Info("Running migrations...")

	// Создаем таблицу миграций, если её нет
	_, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY)`)
	if err != nil {
		return err
	}

	files, err := os.ReadDir("internal/migrations")
	if err != nil {
		return err
	}

	var migrationFiles []string
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".sql" && strings.HasSuffix(f.Name(), ".up.sql") {
			migrationFiles = append(migrationFiles, f.Name())
		}
	}
	sort.Strings(migrationFiles)

	for _, filename := range migrationFiles {
		var exists bool
		err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)", filename).Scan(&exists)
		if err != nil {
			return err
		}

		if exists {
			continue
		}

		slog.Info("Applying migration", slog.String("file", filename))
		content, err := os.ReadFile(filepath.Join("internal/migrations", filename))
		if err != nil {
			return err
		}

		sql := string(content)
		// Убираем BOM (Byte Order Mark) если он есть
		sql = strings.TrimPrefix(sql, "\ufeff")

		_, err = db.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to apply %s: %w", filename, err)
		}

		_, err = db.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", filename)
		if err != nil {
			return err
		}
	}

	slog.Info("Migrations completed successfully")
	return nil
}
