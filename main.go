package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arjun118/fileupload/internal/handlers"
	"github.com/arjun118/fileupload/internal/media/delivery"
	"github.com/arjun118/fileupload/internal/media/minio"
	routingmiddleware "github.com/arjun118/fileupload/internal/middleware"
	"github.com/arjun118/fileupload/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	miniosdk "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	bucketName := "storage"
	endpoint := "minio:9000"
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
	storageProvider := minio.NewStorage(minioClient, bucketName)

	for i := 0; i < 30; i++ {

		err = storageProvider.EnsureBucket(context.Background())

		if err == nil {
			log.Println("connected to minio successfully")
			break
		}

		log.Printf("waiting for minio (%d/30): %v", i+1, err)

		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatalf("failed to initialize bucket: %v", err)
	} else {
		log.Println("ensured bucket...")
	}

	deliverProvider := delivery.NewMinioDelivery(bucketName, "localhost:8080/media")
	videoService := service.NewVideoService(storageProvider, deliverProvider, 3, "minio")
	videoHandler := handlers.NewVideoHandler(videoService)
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("alive\n"))
	})
	r.With(routingmiddleware.CookieAuthenticate).Handle("/media/*", &handlers.RedirectHandler{
		BucketName: bucketName,
	})

	r.Route("/api/videos", func(r chi.Router) {
		r.Use(routingmiddleware.TokenAuthenticate)
		r.Post("/auth-cookie", videoHandler.GetAuthCookie)
		r.Post("/", videoHandler.Upload)   // POST /api/videos
		r.Get("/url", videoHandler.GetURL) // GET /api/videos/url?key=...
		r.Delete("/", videoHandler.Delete) // DELETE /api/videos?key=...
	})

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
