package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/arjun118/fileupload/internal/media"
	"github.com/arjun118/fileupload/service"
)

type VideoHandler struct {
	svc *service.VideoService
}

func NewVideoHandler(svc *service.VideoService) *VideoHandler {
	return &VideoHandler{
		svc: svc,
	}
}

func (v *VideoHandler) Upload(w http.ResponseWriter, r *http.Request) {
	// 50mb is max size
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		http.Error(w, "failed to parse form", http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file is required", http.StatusBadRequest)
	}
	meta := media.FileMetaData{
		Filename:    header.Filename,
		ContentType: header.Header.Get("Content-Type"),
		Size:        header.Size,
	}
	mediaObject, err := v.svc.Upload(r.Context(), file, meta)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(mediaObject)
}

func (h *VideoHandler) GetURL(w http.ResponseWriter, r *http.Request) {
	objectKey := r.URL.Query().Get("key")
	if objectKey == "" {
		http.Error(w, "Missing 'key' query parameter", http.StatusBadRequest)
		return
	}

	url, err := h.svc.GetPlaybackURL(r.Context(), objectKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return just the URL in a quick JSON struct
	response := map[string]string{"playback_url": url}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *VideoHandler) Delete(w http.ResponseWriter, r *http.Request) {
	objectKey := r.URL.Query().Get("key")
	if objectKey == "" {
		http.Error(w, "Missing 'key' query parameter", http.StatusBadRequest)
		return
	}

	if err := h.svc.Delete(r.Context(), objectKey); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "deleted"}\n`))
}
