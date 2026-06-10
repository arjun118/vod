package handlers

import (
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
)

//cache control to be implemented

type ProxyHandler struct {
	MinioClient *minio.Client
	BucketName  string
}

func (p *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet &&
		r.Method != http.MethodHead {
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}
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
	info, err := obj.Stat()
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	contentType := info.ContentType
	if contentType == "" {
		switch filepath.Ext(objectKey) {
		case ".m3u8":
			contentType = "application/vnd.apple.mpegurl"
		case ".ts":
			contentType = "video/mp2t"
		default:
			contentType = "application/octet-stream"
		}
	}
	//segment not modified
	if r.Header.Get("If-None-Match") == info.ETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("ETag", info.ETag)
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = io.Copy(w, obj)
}
