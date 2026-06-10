package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arjun118/fileupload/internal/handlers"
	"github.com/arjun118/fileupload/internal/media/delivery"
	"github.com/arjun118/fileupload/internal/media/minio"
	"github.com/arjun118/fileupload/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	miniosdk "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	bucketName := "storage"
	endpoint := "localhost:9000"
	accessKeyID := "adminpass"
	secretAccessKey := "adminpass"
	useSSL := false
	minioClient, err := miniosdk.New(endpoint, &miniosdk.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("connected to minio successfully")
	storageProvider := minio.NewStorage(minioClient, bucketName)
	if err := storageProvider.EnsureBucket(context.Background()); err != nil {
		log.Fatalf("failed to initialze storage bucket: %w", err)
	}
	deliverProvider := delivery.NewMinioDelivery(bucketName, endpoint)
	videoService := service.NewVideoService(storageProvider, deliverProvider, 3, "minio")
	videoHandler := handlers.NewVideoHandler(videoService)
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Route("/api/videos", func(r chi.Router) {
		r.Post("/", videoHandler.Upload)   // POST /api/videos
		r.Get("/url", videoHandler.GetURL) // GET /api/videos/url?key=...
		r.Delete("/", videoHandler.Delete) // DELETE /api/videos?key=...
	})
	// workDir, _ := os.Getwd()
	// filesDir := http.Dir(filepath.Join(workDir, "storage"))
	// r.Handle("/media/*", http.StripPrefix("/media", http.FileServer(filesDir)))

	server := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		log.Println("server listening on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed to start: %v", err)
		}
	}()
	stop := make(chan os.Signal, 1)

	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	log.Println("Shutdown signal received. Starting graceful shutdown...")

	// 3. Gracefully shutdown the HTTP server first (waits for active HTTP
	// requests to complete)
	// We give it a timeout (e.g., 15 seconds)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.
		Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server Shutdown error: %v", err)
	} else {
		log.Println("HTTP server stopped accepting new connections.")
	}

	log.Println("Closing worker pool queue and waiting for active transcodes to complete...")
	videoService.StopWorkerPool()
	log.Println("Worker pool stopped cleanly.")

	log.Println("Application shutdown complete.")
}
