package handler

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/config"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service" // Импортируем service
	"github.com/go-chi/chi/v5"
)

type Dependencies struct {
	AssetsFS    http.FileSystem
	UserService *service.UserService
	FileService *service.FileService // Добавили FileService
	Config      config.Config
	KafkaWriter KafkaWriter // New interface for Kafka
}

type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...interface{}) error
}

type handlerFunc func(http.ResponseWriter, *http.Request) error

func RegisterRoutes(r *chi.Mux, deps Dependencies) {
	home := homeHandler{}

	auth := AuthHandler{
		UserService: deps.UserService,
		FileService: deps.FileService, // Передаем FileService в AuthHandler
	}

	fileHandler := FileHandler{
		UserService: deps.UserService,
		FileService: deps.FileService, // Передаем FileService
		FilerURL:    deps.Config.FilerURL,
		KafkaWriter: deps.KafkaWriter,
	}

	r.Use(auth.sessionMiddleware)

	r.Get("/", handler(home.handlerIndex))
	r.Get("/about", handler(home.handleAbout))
	r.Get("/register", handler(auth.handleRegisterPage))
	r.Post("/register", handler(auth.handleRegister))
	r.Get("/login", handler(auth.handleLoginPage))
	r.Post("/login", handler(auth.handleLogin))

	r.Group(func(r chi.Router) {
		r.Use(auth.authMiddleware)
		r.Get("/profile", handler(auth.handleProfile))
		r.Get("/preferences", handler(auth.handlePreferences))
		r.Post("/logout", handler(auth.handleLogout))

		r.Post("/profile/files/delete", handler(fileHandler.handleDeleteFile))
		r.Get("/profile/files/download", handler(fileHandler.handleDownloadFile))
		r.Get("/profile/files/data", handler(auth.handleProfileFilesData))
		r.Post("/profile/upload", handler(fileHandler.handleFileUpload))
		r.Get("/api/preferences", handler(fileHandler.handleGetPreferences))
		r.Post("/api/preferences", handler(fileHandler.handleUpsertPreferences))
	})

	r.Post("/api/files/{file_id}/analysis", handler(fileHandler.handleStartAnalysis))
	r.Get("/api/analysis-jobs/{job_id}", handler(fileHandler.handleGetAnalysisJob))
	r.Get("/api/files/{file_id}/analysis", handler(fileHandler.handleGetLatestAnalysis))
	r.Post("/api/chat/threads", handler(fileHandler.handleCreateChatThread))
	r.Get("/api/chat/threads", handler(fileHandler.handleListChatThreads))
	r.Get("/api/chat/threads/{thread_id}/messages", handler(fileHandler.handleGetChatMessages))
	r.Post("/api/chat/threads/{thread_id}/messages", handler(fileHandler.handleCreateChatMessage))
	r.Delete("/api/chat/threads/{thread_id}", handler(fileHandler.handleDeleteChatThread))
	r.Get("/api/chat/jobs/{job_id}", handler(fileHandler.handleGetChatJob))

	r.Handle("/assets/*", http.StripPrefix("/assets", http.FileServer(deps.AssetsFS)))
}

func handler(h handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			handleError(w, r, err)
		}
	}
}

func handleError(w http.ResponseWriter, r *http.Request, err error) {
	slog.Error("error during request", slog.String("err", err.Error()), slog.String("path", r.URL.Path))
	// Отправляем 500 ошибку клиенту, чтобы он не видел пустой экран
	http.Error(w, "Внутренняя ошибка сервера", http.StatusInternalServerError)
}
