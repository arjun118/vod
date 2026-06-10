package middleware

import (
	"encoding/json"
	"maps"
	"net/http"
	"strings"
)

func writeJson(w http.ResponseWriter, status int, data any, headers http.Header) error {
	js, err := json.Marshal(data)
	if err != nil {
		return err
	}
	js = append(js, '\n')
	maps.Copy(w.Header(), headers)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(js)
	return nil
}

func Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 {
			http.Error(w, "missing auth header", http.StatusUnauthorized)
			return
		}

		if parts[0] != "Bearer" {
			http.Error(w, "invalid auth scheme", http.StatusUnauthorized)
			return
		}

		if parts[1] != "secret" {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
