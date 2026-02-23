package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/config"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/handler"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service" // Импортируем service
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

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

    r := chi.NewRouter()
    handler.RegisterRoutes(r, handler.Dependencies{
        AssetsFS:    http.Dir(cfg.AssetsDir),
        UserService: userService,
        FileService: fileService,
        Config:      cfg,
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