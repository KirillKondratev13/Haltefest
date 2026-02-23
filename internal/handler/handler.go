package handler

import (
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
        r.Post("/logout", handler(auth.handleLogout))

        r.Post("/profile/files/delete", handler(fileHandler.handleDeleteFile))
        r.Get("/profile/files/download", handler(fileHandler.handleDownloadFile))
        r.Post("/profile/upload", handler(fileHandler.handleFileUpload))
    })

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