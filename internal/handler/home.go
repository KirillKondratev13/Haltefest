package handler

import (
	"net/http"

	"github.com/Cyr1ll/golang-templ-htmx-app/internal/view/home"
)

type homeHandler struct{}

func (h *homeHandler) handlerIndex(w http.ResponseWriter, r *http.Request) error {
    user := getUserFromContext(r)
    return home.Index(user).Render(r.Context(), w)
}

func (h *homeHandler) handleAbout(w http.ResponseWriter, r *http.Request) error {
    user := getUserFromContext(r)
    return home.About(user).Render(r.Context(), w)
}
