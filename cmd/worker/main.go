package main

import (
	"context"
	"errors"
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
	"github.com/arjun118/fileupload/internal/outbox"
	"github.com/arjun118/fileupload/internal/queue"
	"github.com/arjun118/fileupload/internal/transcode"
	"github.com/arjun118/fileupload/internal/transcoder"
	"github.com/arjun118/fileupload/service"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

func StartWorkerPool(ctx context.Context, v *service.VideoService, queue queue.Queue, dlq queue.Queue, db *pgxpool.Pool, jobs <-chan queue.Message, workerLogger zerolog.Logger, workerWg *sync.WaitGroup, transcodeRepo *transcode.Repository, maxWorkers int) {
	for i := range maxWorkers {
		workerWg.Add(1)
		go func(workerID string) {
			defer workerWg.Done()
			workerLoggerSpecific := workerLogger.With().Str("worker_id", workerID).Logger()
			// Replace log.Printf with structured zerolog Info
			workerLoggerSpecific.Info().Msg("starting transcode worker")

			for {
				select {
				case <-ctx.Done():
					workerLoggerSpecific.Info().Msg("worker shutting down (context cancelled)")
					return

				case message, ok := <-jobs:
					if !ok {
						workerLoggerSpecific.Info().Msg("worker shutting down (channel closed)")
						return
					}
					// Create a sub-logger specifically for this job (attaches video_id and worker_id to all subsequent logs)
					jobLogger := workerLoggerSpecific.With().
						Str("job_id", message.JobID).
						Logger()
					jobLogger.Info().Msg("processing video")

					start := time.Now()
					// get the job metadata from pq
					execInfo, err := transcodeRepo.GetExecutionInfo(ctx, db, message.JobID)
					if err != nil {
						jobLogger.Error().Err(err).Msg("failed to get execution info from db")
						continue
					}

					// proceed basis the status
					if execInfo.Job.IsCompleted() || execInfo.Job.IsDead() {
						_ = queue.Ack(ctx, message.ID)
						continue
					}
					if execInfo.Job.IsFailed() && !execInfo.Job.CanRetry() {
						// mark job dead
						execInfo.Job.Status = transcode.StatusDead
						if err := transcodeRepo.Update(ctx, db, &execInfo.Job); err != nil {
							jobLogger.Error().Err(err).Str("job_id", execInfo.Job.ID.String()).Msg("failed to mark job dead")
							continue
						}
						// push to dlq
						if _, err := dlq.Publish(ctx, execInfo.Job.ID.String()); err != nil {
							jobLogger.Error().
								Err(err).
								Msg("failed to publish job to DLQ")
							continue
						}
						// ack
						_ = queue.Ack(ctx, message.ID)
						continue
					}
					//if job  is stale/pending/failed and can retry - process it
					if err := transcodeRepo.MarkProcessing(ctx, db, execInfo.Job.ID, workerID); err != nil {
						continue
					}
					// change in memory job state
					execInfo.Job.Attempts++
					execInfo.Job.Status = transcode.StatusProcessing
					worker := workerID
					execInfo.Job.ProcessedBy = &worker

					jobCtx, jobCancel := context.WithTimeout(context.Background(), 10*time.Minute)
					keys := media.VideoKeys{
						RawObjectKey: execInfo.SourceKey,
						PlaylistKey:  execInfo.PlaylistKey,
						StreamFolder: path.Dir(execInfo.PlaylistKey),
					}
					transCodeStart := time.Now()
					err = v.ProcessAndSaveHLS(jobCtx, execInfo.Job.VideoID.String(), keys)
					jobCancel()
					duration := time.Since(transCodeStart)
					// transcoding failed for some reason
					// decide below whether to retry to mark dead
					if err != nil {
						jobLogger.Error().
							Err(err).
							Dur("duration", duration).
							Msg("video transcoding failed")
						var transErr *transcoder.TranscodeError
						severity := transcoder.SeverityTransient
						var errorStr string
						if errors.As(err, &transErr) {
							severity = transcoder.ClassifyError(transErr.Err, transErr.Stderr)
							errorStr = transErr.Stderr
						} else {
							severity = transcoder.ClassifyError(err, "")
							errorStr = err.Error()
						}

						switch severity {
						case transcoder.SeverityPermanent:
							// failure is  permanent - no use retrying
							jobLogger.Error().
								Str("severity", "permanent").
								Msg("marking job as permanently failed")
							// mark dead and push to dlq
							execInfo.Job.MarkDead(errors.New(errorStr))
							if err := transcodeRepo.Update(ctx, db, &execInfo.Job); err != nil {
								jobLogger.Error().Err(err).Str("job_id", execInfo.Job.ID.String()).Msg("failed to mark job dead")
								continue
							}
							if _, err := dlq.Publish(ctx, execInfo.Job.ID.String()); err != nil {
								jobLogger.Error().
									Err(err).
									Msg("failed to publish job to DLQ")
								continue
							}
							_ = queue.Ack(ctx, message.ID)
							continue

						case transcoder.SeverityTransient:
							execInfo.Job.MarkFailed(errors.New(errorStr))
							if err := transcodeRepo.Update(ctx, db, &execInfo.Job); err != nil {
								jobLogger.Error().Err(err).Str("job_id", execInfo.Job.ID.String()).Msg("failed to mark job failed")
								continue
							}
							jobLogger.Warn().
								Str("severity", "transient").
								Msg("transcoding failed with transient error; will retry")
							// return error so your queue/worker retry mechanism handles it
							continue
						}
					} else {
						jobLogger.Info().Dur("duration", duration).Msg("video transcoding + stream upload to storage completed successfully")
						execInfo.Job.MarkCompleted()
					}
					if err := transcodeRepo.Update(ctx, db, &execInfo.Job); err != nil {
						continue
					}
					if execInfo.Job.IsCompleted() {
						if err := queue.Ack(ctx, message.ID); err != nil {
							jobLogger.Error().Err(err).Msg("failed to acknowledge completed message")
							// continue
						} else {
							jobLogger.Info().Dur("duration", time.Since(start)).Msg("job completed end-to-end successfully")
						}
					}
				}
			}

		}(fmt.Sprintf("worker_%d", i+1))
	}
}

func StopWorkerPool(workerWg *sync.WaitGroup) {
	workerWg.Wait()
}

func main() {
	cfg := config.Load()
	// setup db connection
	dbPool, err := infra.NewPostgresPool(cfg)
	baseLogger := logger.New(os.Stderr)

	// create a buffered channel
	// the stream consumer consumes from redis and pushes it to this channel
	// for our transcoding worker to consume
	jobChannel := make(chan queue.Message, 20)

	// Correctly tag the worker logger component
	workerLogger := baseLogger.With().Str("component", "transcode_worker").Logger()
	videoServiceLogger := baseLogger.With().Str("component", "video_service").Logger()
	outboxLogger := baseLogger.With().Str("component", "outbox_service").Logger()
	// create outbox repos instances
	outboxRepo := outbox.NewRepository()
	transcodeRepo := transcode.NewRepository()

	if err != nil {
		baseLogger.Error().Err(err).Msg("failed to initiate db connection")
	} else {
		baseLogger.Info().Msg("DB connection pool is initiated")
	}
	// Initialize Clients
	minioClient, _ := infra.NewMinioClient(cfg)
	redisClient := infra.NewRedisClient(cfg)

	transcodeQueue := queue.NewRedisQueue(redisClient, cfg.TranscodeStreamName, cfg.TranscodeGroupName, cfg.TranscodeConsumerName)

	transcodeDLQ := queue.NewRedisQueue(redisClient, fmt.Sprintf("%s_dlq", cfg.TranscodeStreamName), fmt.Sprintf("%s_dlq", cfg.TranscodeGroupName), fmt.Sprintf("%s_dlq", cfg.TranscodeConsumerName))
	if err := transcodeQueue.Init(context.Background()); err != nil {
		baseLogger.Fatal().Err(err).Msg("failed to initialize transcode stream stream and consumer group")
	}
	if err := transcodeDLQ.Init(context.Background()); err != nil {
		baseLogger.Fatal().Err(err).Msg("failed to initialize transcode dlq stream and consumer group")
	}
	// Providers
	storageProvider := minio.NewStorage(minioClient, cfg.RawBucketName, cfg.StreamsBucketName)
	deliverProvider := delivery.NewMinioDelivery(cfg.StreamsBucketName, "localhost:8080/media")

	storageLayout := media.NewStorageLayout("videos")

	videoService := service.NewVideoService(storageProvider, deliverProvider, dbPool, transcodeQueue, videoServiceLogger, storageLayout, nil, transcodeRepo, outboxRepo, "minio")

	var wg sync.WaitGroup
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer stop()
	// start transcode worker pool
	// ctx context.Context, v *service.VideoService, queue queue.Queue, db *pgxpool.Pool, jobs <-chan queue.Message, workerLogger zerolog.Logger, workerWg *sync.WaitGroup, maxWorkers int
	workerLogger.Info().Msg("starting transcode worker pool")
	go StartWorkerPool(ctx, videoService, transcodeQueue, transcodeDLQ, dbPool, jobChannel, workerLogger, &wg, transcodeRepo, cfg.MaxTranscodeWorkers)
	// defer StopWorkerPool(&wg)
	// start outbox publisher - [outbox table] -> redis stream
	workerLogger.Info().Msg("starting outbox publisher loop")
	go outbox.StartOutboxPublisher(ctx, outboxLogger, dbPool, outboxRepo, transcodeQueue)
	// start stream consumer - [from redis stream] -> a buffered channel
	workerLogger.Info().Msg("starting redis consumer loop")
	go StartRedisStreamConsumer(ctx, "reader-1", workerLogger, transcodeQueue, jobChannel)
	go StartRedisPELConsumer(ctx, workerLogger, transcodeQueue, transcodeDLQ, transcodeRepo, dbPool, jobChannel)
	<-ctx.Done()

	workerLogger.Info().Msg("Shutdown signal received. Waiting for active transcodes to complete...")

	// Wait for all workers to finish their active job before exiting
	wg.Wait()
	dbPool.Close()
	redisClient.Close()
	workerLogger.Info().Msg("All workers finished. Worker pool stopped cleanly.")
	// Start reading from redisClient and call videoService.Transcode...
}
