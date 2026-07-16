package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/arjun118/fileupload/internal/config"
	"github.com/arjun118/fileupload/internal/infra"
	"github.com/arjun118/fileupload/internal/media/delivery"
	"github.com/arjun118/fileupload/internal/media/minio"
	"github.com/arjun118/fileupload/internal/queue"
	"github.com/arjun118/fileupload/service"
)

func StartWorkerPool(ctx context.Context, v *service.VideoService, workerWg *sync.WaitGroup, maxWorkers int) {
	for i := range maxWorkers {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			log.Printf("starting transcode worker %d", workerID)

			for {
				job, err := v.Queue.Consume(ctx)
				if err != nil {
					// chck if the error is because context is cancelled
					if ctx.Err() != nil {
						log.Printf("worker %d shutting down (context cancelled)", workerID)
						return
					}
					log.Printf("worker %d queue error: %v", workerID, err)
					time.Sleep(1 * time.Second)
					continue
				}
				log.Printf("worker %d processing video: %s", workerID, job.VideoID)
				jobCtx, jobCancel := context.WithTimeout(context.Background(), 10*time.Minute)
				workDir := v.WorkDir(job.VideoID)
				output := filepath.Join(workDir, "output")
				err = v.ProcessAndSaveHLS(jobCtx, job.VideoID, job.StorageSourceKey, output)
				if err != nil {
					log.Printf("[Error] Transocde Job %s failed: %v", job.VideoID, err)
				} else {
					log.Printf("Job %s completed successfully", job.VideoID)
				}
				jobCancel()
			}
		}(i)
	}
}

func StopWorkerPool(workerWg *sync.WaitGroup) {
	workerWg.Wait()
}

func main() {
	cfg := config.Load()

	// Initialize Clients
	minioClient, _ := infra.NewMinioClient(cfg)
	redisClient := infra.NewRedisClient(cfg)
	transcodeQueue := queue.NewRedisQueue(redisClient, cfg.TranscodeQueueName)
	// Providers
	storageProvider := minio.NewStorage(minioClient, cfg.RawBucketName, cfg.StreamsBucketName)
	deliverProvider := delivery.NewMinioDelivery(cfg.StreamsBucketName, "localhost:8080/media")

	videoService := service.NewVideoService(storageProvider, deliverProvider, transcodeQueue, "minio")
	log.Println("worker is running")
	var wg sync.WaitGroup

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	StartWorkerPool(ctx, videoService, &wg, cfg.MaxTranscodeWorkers)
	<-ctx.Done()
	log.Println("Shutdown signal received. Waiting for active transcodes to complete...")

	// Wait for all workers to finish their active job before exiting
	wg.Wait()
	log.Println("All workers finished. Worker pool stopped cleanly.")
	defer StopWorkerPool(&wg)
	// Start reading from redisClient and call videoService.Transcode...
}
