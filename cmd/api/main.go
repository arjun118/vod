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
	"github.com/arjun118/fileupload/internal/outbox"
	"github.com/arjun118/fileupload/internal/queue"
	"github.com/arjun118/fileupload/internal/transcode"
	"github.com/arjun118/fileupload/internal/video"
	"github.com/arjun118/fileupload/service"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {

	// setup repos
	//
	videoRepo := video.NewRepository()
	transcodeRepo := transcode.NewRepository()
	outboxRepo := outbox.NewRepository()

	baseLogger := logger.New(os.Stdout)
	apiLogger := baseLogger.With().Str("component", "api").Logger()
	videoServiceLogger := baseLogger.With().Str("component", "video_service").Logger()
	cfg := config.Load()

	// establish db connection
	dbPool, dberr := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if dberr != nil {
		apiLogger.Error().Err(dberr).Msg("failed to establish db connection")
		return
	} else {
		apiLogger.Info().Msg("DB connection established")
	}
	defer dbPool.Close()

	minioClient, _ := infra.NewMinioClient(cfg)
	redisClient := infra.NewRedisClient(cfg)
	transcodeQueue := queue.NewRedisQueue(redisClient, cfg.TranscodeStreamName, cfg.TranscodeGroupName, cfg.TranscodeConsumerName)

	storageProvider := minio.NewStorage(minioClient, cfg.RawBucketName, cfg.StreamsBucketName)
	deliverProvider := delivery.NewMinioDelivery(cfg.StreamsBucketName, cfg.NginxDeliveryEndpoint)
	var err error
	for i := range 30 {

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
	videoService := service.NewVideoService(storageProvider, deliverProvider, dbPool, transcodeQueue, videoServiceLogger, storageLayout, videoRepo, transcodeRepo, outboxRepo, "minio")
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
	appCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-appCtx.Done()
	apiLogger.Info().Msg("shutdown signal received, Shutdown signal received. Starting graceful shutdown...")
	// Gracefully shutdown the HTTP server first (waits for active HTTP
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

	apiLogger.Info().Msg("database connection pool is closed")

	if err := redisClient.Close(); err != nil {
		apiLogger.Error().Err(err).Msg("failed to close redis client")
	} else {
		apiLogger.Info().Msg("redis client closed cleanly")
	}

	log.Println("Worker pool stopped cleanly.")

	log.Println("Application shutdown complete.")
}
