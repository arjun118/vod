package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/arjun118/fileupload/internal/config"
	"github.com/arjun118/fileupload/internal/infra"
	"github.com/arjun118/fileupload/internal/logger"
	"github.com/arjun118/fileupload/internal/media"
	"github.com/arjun118/fileupload/internal/media/delivery"
	"github.com/arjun118/fileupload/internal/media/minio"
	"github.com/arjun118/fileupload/internal/queue"
	"github.com/arjun118/fileupload/service"
	"github.com/rs/zerolog"
)

func StartWorkerPool(ctx context.Context, v *service.VideoService, workerLogger zerolog.Logger, workerWg *sync.WaitGroup, maxWorkers int) {
	for i := range maxWorkers {
		workerWg.Add(1)
		go func(workerID string) {
			defer workerWg.Done()

			// Replace log.Printf with structured zerolog Info
			workerLogger.Info().Str("worker_id", workerID).Msg("starting transcode worker")

			for {
				job, err := v.Queue.Consume(ctx)
				if err != nil {
					if ctx.Err() != nil {
						// Log context cancellation gracefully
						workerLogger.Info().Str("worker_id", workerID).Msg("worker shutting down (context cancelled)")
						return
					}
					// Log queue errors with structured Error() and attach the err
					workerLogger.Error().Err(err).Str("worker_id", workerID).Msg("queue error")
					time.Sleep(1 * time.Second)
					continue
				}

				// Create a sub-logger specifically for this job (attaches video_id and worker_id to all subsequent logs)
				jobLogger := workerLogger.With().
					Str("video_id", job.VideoID).
					Str("worker_id", workerID).
					Logger()

				jobLogger.Info().Msg("processing video")

				jobCtx, jobCancel := context.WithTimeout(context.Background(), 10*time.Minute)
				start := time.Now()
				keys := media.VideoKeys{
					RawObjectKey: job.StorageSourceKey,
					PlaylistKey:  job.PlaylistKey,
					StreamFolder: path.Dir(job.PlaylistKey),
				}
				err = v.ProcessAndSaveHLS(jobCtx, job.VideoID, keys)
				duration := time.Since(start)
				if err != nil {
					// Use jobLogger.Error() for failures
					jobLogger.Error().Err(err).Dur("duration", duration).Msg("transcode job failed")
				} else {
					// Use jobLogger.Info() for success
					jobLogger.Info().Dur("duration", duration).Msg("transcode job completed successfully")
				}
				jobCancel()
			}
		}(fmt.Sprintf("worker_%d", i+1))
	}
}

func StopWorkerPool(workerWg *sync.WaitGroup) {
	workerWg.Wait()
}

func main() {
	cfg := config.Load()
	baseLogger := logger.New(os.Stderr)
	// Correctly tag the worker logger component
	workerLogger := baseLogger.With().Str("component", "transcode_worker").Logger()
	videoServiceLogger := baseLogger.With().Str("component", "video_service").Logger()

	// Initialize Clients
	minioClient, _ := infra.NewMinioClient(cfg)
	redisClient := infra.NewRedisClient(cfg)
	transcodeQueue := queue.NewRedisQueue(redisClient, cfg.TranscodeQueueName)

	// Providers
	storageProvider := minio.NewStorage(minioClient, cfg.RawBucketName, cfg.StreamsBucketName)
	deliverProvider := delivery.NewMinioDelivery(cfg.StreamsBucketName, "localhost:8080/media")

	storageLayout := media.NewStorageLayout("videos")
	videoService := service.NewVideoService(storageProvider, deliverProvider, transcodeQueue, videoServiceLogger, storageLayout, "minio")

	workerLogger.Info().Msg("worker is running")
	var wg sync.WaitGroup

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	StartWorkerPool(ctx, videoService, workerLogger, &wg, cfg.MaxTranscodeWorkers)
	<-ctx.Done()

	workerLogger.Info().Msg("Shutdown signal received. Waiting for active transcodes to complete...")

	// Wait for all workers to finish their active job before exiting
	wg.Wait()
	workerLogger.Info().Msg("All workers finished. Worker pool stopped cleanly.")
	defer StopWorkerPool(&wg)
	// Start reading from redisClient and call videoService.Transcode...
}
