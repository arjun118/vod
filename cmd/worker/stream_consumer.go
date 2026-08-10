package main

import (
	"context"
	"time"

	"github.com/arjun118/fileupload/internal/queue"
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
