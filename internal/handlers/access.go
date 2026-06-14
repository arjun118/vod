package handlers

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
)

type RedirectHandler struct {
	BucketName string
}

func (a *RedirectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cleanPath := filepath.Clean(strings.TrimPrefix(r.URL.Path,
		"/media/"))
	if strings.HasPrefix(cleanPath, "..") {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	redirect := fmt.Sprintf("/_protected/%s/%s", a.BucketName,
		cleanPath)

	w.Header().Set("X-Accel-Redirect", redirect)
	w.WriteHeader(http.StatusOK)
}
