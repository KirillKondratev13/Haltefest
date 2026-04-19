package handler

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/service"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/view/home"
	"github.com/Cyr1ll/golang-templ-htmx-app/internal/view/layout"
	"github.com/a-h/templ"
	"golang.org/x/crypto/bcrypt"
)

type FileData struct {
	Files    []FileJSON `json:"files"`
	Username string     `json:"username"`
}

type FileJSON struct {
	ID           int    `json:"ID"`
	FileName     string `json:"FileName"`
	FileType     string `json:"FileType"`
	FileSize     int64  `json:"FileSize"`
	CreatedAt    string `json:"CreatedAt"`
	DownloadURL  string `json:"DownloadURL"`
	DeleteURL    string `json:"DeleteURL"`
	Status       string `json:"Status"`
	Tag          string `json:"Tag"`
	FailureCause string `json:"FailureCause"`
}

type AuthHandler struct {
	UserService *service.UserService
	FileService *service.FileService // Добавили FileService
}

type contextKey string

const userContextKey = contextKey("user")

func (h *AuthHandler) handleRegisterPage(w http.ResponseWriter, r *http.Request) error {
	return home.Register().Render(r.Context(), w)
}

func (h *AuthHandler) handleRegister(w http.ResponseWriter, r *http.Request) error {
	username := r.FormValue("username")
	email := r.FormValue("email")
	password := r.FormValue("password")

	passwordHash, err := hashPassword(password)
	if err != nil {
		return err
	}

	_, err = h.UserService.CreateUser(r.Context(), username, email, passwordHash)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			http.Error(w, "User with this email already exists", http.StatusBadRequest)
			return nil
		}
		return err
	}

	http.Redirect(w, r, "/login", http.StatusSeeOther)
	return nil
}

func (h *AuthHandler) handleLoginPage(w http.ResponseWriter, r *http.Request) error {
	return home.Login().Render(r.Context(), w)
}

func (h *AuthHandler) handleLogin(w http.ResponseWriter, r *http.Request) error {
	email := r.FormValue("email")
	password := r.FormValue("password")

	var user service.User
	err := h.UserService.DB.QueryRow(r.Context(), "SELECT id, username, email, password_hash FROM users WHERE email = $1", email).Scan(&user.ID, &user.Username, &user.Email, &user.PasswordHash)
	if err != nil {
		http.Error(w, "Invalid email or password", http.StatusUnauthorized)
		return nil
	}

	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password))
	if err != nil {
		http.Error(w, "Invalid email or password", http.StatusUnauthorized)
		return nil
	}

	token := generateToken()
	_, err = h.UserService.DB.Exec(r.Context(), "INSERT INTO sessions (user_id, token) VALUES ($1, $2)", user.ID, token)
	if err != nil {
		return err
	}

	http.SetCookie(w, &http.Cookie{
		Name:    "session_token",
		Value:   token,
		Expires: time.Now().Add(24 * time.Hour),
	})

	http.Redirect(w, r, "/profile", http.StatusSeeOther)
	return nil
}

func (h *AuthHandler) handleLogout(w http.ResponseWriter, r *http.Request) error {
	cookie, err := r.Cookie("session_token")
	if err != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return nil
	}

	// Удаляем сессию из БД
	_, err = h.UserService.DB.Exec(r.Context(), "DELETE FROM sessions WHERE token = $1", cookie.Value)
	if err != nil {
		return err
	}

	// Очищаем куки
	http.SetCookie(w, &http.Cookie{
		Name:    "session_token",
		Value:   "",
		Expires: time.Unix(0, 0),
	})

	http.Redirect(w, r, "/", http.StatusSeeOther)
	return nil
}
func (h *AuthHandler) sessionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("session_token")
		if err == nil {
			var user service.User
			err = h.UserService.DB.QueryRow(r.Context(),
				"SELECT u.id, u.username, u.email FROM users u JOIN sessions s ON u.id = s.user_id WHERE s.token = $1",
				cookie.Value,
			).Scan(&user.ID, &user.Username, &user.Email)
			if err == nil {
				r = r.WithContext(context.WithValue(r.Context(), userContextKey, &user))
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (h *AuthHandler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := getUserFromContext(r)
		if user == nil {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *AuthHandler) handleProfile(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return nil
	}

	files, fileData, err := h.buildFileData(r.Context(), user)
	if err != nil {
		return err
	}

	// Подготовим URL для каждого файла
	for i := range files {
		files[i].DownloadURL = fmt.Sprintf("/profile/files/download?file_id=%d", files[i].ID)
		files[i].DeleteURL = fmt.Sprintf("/profile/files/delete?file_id=%d", files[i].ID)
	}

	return home.Profile(user, files, fileData).Render(r.Context(), w) // Теперь передаём *service.User
}

func (h *AuthHandler) handlePreferences(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return nil
	}

	return layout.Base(layout.BaseProps{
		Title: "Preferences",
		User:  user,
	}).Render(
		templ.WithChildren(r.Context(), templ.ComponentFunc(func(_ context.Context, writer io.Writer) error {
			_, err := io.WriteString(writer, `<main><div id="preferences-page-root" class="w-full"></div></main>`)
			return err
		})),
		w,
	)
}

func (h *AuthHandler) handleProfileFilesData(w http.ResponseWriter, r *http.Request) error {
	user := getUserFromContext(r)
	if user == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil
	}

	_, fileData, err := h.buildFileData(r.Context(), user)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(fileData); err != nil {
		return err
	}
	return nil
}

func (h *AuthHandler) buildFileData(ctx context.Context, user *service.User) ([]service.UserFile, FileData, error) {
	files, err := h.FileService.GetUserFiles(ctx, user.ID)
	if err != nil {
		return nil, FileData{}, err
	}

	filesJSON := make([]FileJSON, 0, len(files))
	for _, f := range files {
		filesJSON = append(filesJSON, FileJSON{
			ID:           f.ID,
			FileName:     f.FileName,
			FileType:     f.FileType,
			FileSize:     f.FileSize,
			CreatedAt:    f.CreatedAt.Format("2006-01-02 15:04:05"),
			DownloadURL:  fmt.Sprintf("/profile/files/download?file_id=%d", f.ID),
			DeleteURL:    fmt.Sprintf("/profile/files/delete?file_id=%d", f.ID),
			Status:       stringFromPtr(f.Status),
			Tag:          stringFromPtr(f.Tag),
			FailureCause: stringFromPtr(f.FailureCause),
		})
	}

	fileData := FileData{
		Files:    filesJSON,
		Username: user.Username,
	}
	return files, fileData, nil
}

func stringFromPtr(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func getUserFromContext(r *http.Request) *service.User {
	user, ok := r.Context().Value(userContextKey).(*service.User)
	if !ok {
		return nil
	}
	return user
}

func generateToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

func hashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
