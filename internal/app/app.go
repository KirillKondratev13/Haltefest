package app

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/config"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/handler"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service" // Импортируем service
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

    // Создаем сервисы
    userService := &service.UserService{DB: dbpool}
    fileService := &service.FileService{
    DB:        dbpool, // передаем пул напрямую
    MasterURL: cfg.SeaweedFSMasterURL + "/dir",
    VolumeURL: cfg.SeaweedFSVolumeURL,
}
    r := chi.NewRouter()
    handler.RegisterRoutes(r, handler.Dependencies{
        AssetsFS:     http.Dir(cfg.AssetsDir),
        UserService:  userService,
        FileService:  fileService, // передаём готовый сервис
    })

    s := http.Server{
        Addr:    cfg.ServerAddr,
        Handler: r,
    }

    go func() {
        <-ctx.Done()
        slog.Info("shutting down server")
        s.Shutdown(ctx)
    }()

    slog.Info("starting server", slog.String("add", cfg.ServerAddr))
    if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        return err
    }

    return nil
}