package main

import (
	"context"
	"time"

	"github.com/arjun118/fileupload/internal/queue"
	"github.com/arjun118/fileupload/internal/transcode"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

func StartRedisStreamConsumer(ctx context.Context, reader string, logger zerolog.Logger, queue queue.Queue, jobs chan<- queue.Message) {
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("stream consumer stopping")
			return
		default:
		}
		messages, err := queue.Consume(ctx, 10)
		if err != nil {
			logger.Error().Err(err).Msg("failed to consume from stream, waiting for 2 second before resuming...")
			time.Sleep(2 * time.Second)
			continue
		}
		if len(messages) == 0 {
			// logger.Warn().Msg("got empty list of jobs")
			continue
		}
		for _, message := range messages {
			select {
			case <-ctx.Done():
				return
			case jobs <- message:
			}
		}
	}
}

func retryBackoff(attempts int) time.Duration {
	switch attempts {
	case 1:
		return 1 * time.Minute
	case 2:
		return 5 * time.Minute
	case 3:
		return 15 * time.Minute
	default:
		return 30 * time.Minute
	}
}

func StartRedisPELConsumer(
	ctx context.Context,
	logger zerolog.Logger,
	queue queue.Queue,
	dlq queue.Queue,
	repo *transcode.Repository,
	db *pgxpool.Pool,
	jobs chan<- queue.Message,
) {
	minIdleTime := 15 * time.Minute
	// every 5 mins we will run
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:

			// Claim messages that have been idle in PEL for at least 20 minute.
			// Real retry eligibility is checked below using retryBackoff().
			msgs, err := queue.ClaimStaleJobs(ctx, minIdleTime, 100)
			if err != nil {
				logger.Error().Err(err).Msg("failed to claim stale jobs")
				continue
			}

			for _, msg := range msgs {

				execInfo, err := repo.GetExecutionInfo(ctx, db, msg.JobID)
				if err != nil {
					logger.Error().
						Err(err).
						Str("job_id", msg.JobID).
						Msg("failed to load job state")
					continue
				}

				job := &execInfo.Job

				switch {

				// ---------------------------------------------------------
				// Already completed: ACK stale PEL message and forget it.
				// ---------------------------------------------------------
				case job.IsCompleted():
					if err := queue.Ack(ctx, msg.ID); err != nil {
						logger.Error().
							Err(err).
							Str("job_id", job.ID.String()).
							Msg("failed to ack completed message")
					}

				// ---------------------------------------------------------
				// Permanently failed: send to DLQ and ACK original message.
				// ---------------------------------------------------------
				case job.IsFailed() && !job.CanRetry():

					if _, err := dlq.Publish(ctx, job.ID.String()); err != nil {
						logger.Error().
							Err(err).
							Str("job_id", job.ID.String()).
							Msg("failed to publish job to DLQ")
						continue
					}

					if err := queue.Ack(ctx, msg.ID); err != nil {
						logger.Error().
							Err(err).
							Str("job_id", job.ID.String()).
							Msg("failed to ack DLQ message")
					}

				// ---------------------------------------------------------
				// Failed but retryable: respect retry backoff.
				// ---------------------------------------------------------
				case job.IsFailed() && job.CanRetry():

					required := retryBackoff(job.Attempts)

					if msg.Idle < required {
						logger.Info().
							Str("job_id", job.ID.String()).
							Dur("idle", msg.Idle).
							Dur("required", required).
							Msg("retry backoff not reached yet")
						continue
					}

					if _, err := queue.Publish(ctx, job.ID.String()); err != nil {
						logger.Error().
							Err(err).
							Str("job_id", job.ID.String()).
							Msg("failed to requeue job")
						continue
					}

					if err := queue.Ack(ctx, msg.ID); err != nil {
						logger.Error().
							Err(err).
							Str("job_id", job.ID.String()).
							Msg("failed to ack retried message")
					}

				// ---------------------------------------------------------
				// Stale processing job: worker probably died.
				// ---------------------------------------------------------
				case job.IsProcessing():

					if msg.Idle < 30*time.Minute {
						continue
					}

					job.Status = transcode.StatusStale
					job.ProcessedBy = nil

					if err := repo.Update(ctx, db, job); err != nil {
						continue
					}

					_, _ = queue.Publish(ctx, job.ID.String())
					_ = queue.Ack(ctx, msg.ID)
				}
			}
		}
	}
}
