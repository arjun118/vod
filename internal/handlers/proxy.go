package handlers

import (
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
)

type ProxyHandler struct {
	MinioClient *minio.Client
	BucketName  string
}

func (p *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	objectKey := strings.TrimPrefix(r.URL.Path, "/media/")
	if objectKey == "" {
		http.Error(w, "missing object key", http.StatusBadRequest)
		return
	}

	obj, err := p.MinioClient.GetObject(r.Context(), p.BucketName, objectKey, minio.GetObjectOptions{})
	if err != nil {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	defer obj.Close()
	var contentType string
	switch filepath.Ext(objectKey) {
	case ".m3u8":
		contentType = "application/vnd.apple.mpegurl"
	case ".ts":
		contentType = "video/mp2t"
	default:
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)

	_, _ = io.Copy(w, obj)
}
