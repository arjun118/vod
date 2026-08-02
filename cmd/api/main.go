package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/arjun118/fileupload/internal/config"
	"github.com/arjun118/fileupload/internal/handlers"
	"github.com/arjun118/fileupload/internal/infra"
	"github.com/arjun118/fileupload/internal/logger"
	"github.com/arjun118/fileupload/internal/media"
	"github.com/arjun118/fileupload/internal/media/delivery"
	"github.com/arjun118/fileupload/internal/media/minio"
	routingmiddleware "github.com/arjun118/fileupload/internal/middleware"
	"github.com/arjun118/fileupload/internal/queue"
	"github.com/arjun118/fileupload/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {

	baseLogger := logger.New(os.Stdout)
	apiLogger := baseLogger.With().Str("component", "api").Logger()
	videoServiceLogger := baseLogger.With().Str("component", "video_service").Logger()
	cfg := config.Load()

	minioClient, _ := infra.NewMinioClient(cfg)
	redisClient := infra.NewRedisClient(cfg)
	transcodeQueue := queue.NewRedisQueue(redisClient, cfg.TranscodeQueueName)

	storageProvider := minio.NewStorage(minioClient, cfg.RawBucketName, cfg.StreamsBucketName)
	deliverProvider := delivery.NewMinioDelivery(cfg.StreamsBucketName, cfg.NginxDeliveryEndpoint)
	var err error
	for i := 0; i < 30; i++ {

		err = storageProvider.EnsureBuckets(context.Background())

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
	storageLayout := media.NewStorageLayout("videos")
	videoService := service.NewVideoService(storageProvider, deliverProvider, transcodeQueue, videoServiceLogger, storageLayout, "minio")
	videoHandler := handlers.NewVideoHandler(videoService)
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(routingmiddleware.LoggerMiddleware(apiLogger))
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("alive\n"))
	})
	r.With(routingmiddleware.CookieAuthenticate).Handle("/media/*", &handlers.RedirectHandler{
		BucketName: cfg.StreamsBucketName,
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

	log.Println("Worker pool stopped cleanly.")

	log.Println("Application shutdown complete.")
}
