package handler

import (
	"log/slog"
	"net/http"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service" // Импортируем service
	"github.com/go-chi/chi/v5"
)

type Dependencies struct {
    AssetsFS   http.FileSystem
    UserService *service.UserService // Используем UserService из service
    FileService *service.FileService
}

type handlerFunc func(http.ResponseWriter, *http.Request) error

func RegisterRoutes(r *chi.Mux, deps Dependencies) {
    home := homeHandler{}
    // fileService := &service.FileService{
    //     DB:          deps.UserService.DB,
    //     FileService: deps.FileService,
    // }
    
    auth := AuthHandler{
        UserService: deps.UserService,
        FileService: deps.FileService,
    }

    // Добавляем sessionMiddleware ко всем маршрутам
    r.Use(auth.sessionMiddleware)

    r.Get("/", handler(home.handlerIndex))
    r.Get("/about", handler(home.handleAbout))
    r.Get("/register", handler(auth.handleRegisterPage))
    r.Post("/register", handler(auth.handleRegister))
    r.Get("/login", handler(auth.handleLoginPage))
    r.Post("/login", handler(auth.handleLogin))

    // Защищенные маршруты
    r.Group(func(r chi.Router) {
        r.Use(auth.authMiddleware)
        r.Get("/profile", handler(auth.handleProfile))
        r.Post("/logout", handler(auth.handleLogout))

        // Новые эндпоинты для работы с файлами
        r.Post("/upload", handler(auth.handleUploadFile))      // ✅ Добавили
        r.Get("/files", handler(auth.handleListFiles))         // ✅ Добавили
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
    slog.Error("error during request", slog.String("err", err.Error()))
}