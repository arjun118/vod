package outbox

import (
	"context"
	"time"

	"github.com/arjun118/fileupload/internal/queue"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

func StartOutboxPublisher(ctx context.Context, outboxLogger zerolog.Logger, db *pgxpool.Pool, outBox *Repository, q queue.Queue) {
	limit := 10
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		tx, err := db.Begin(ctx)
		if err != nil {
			outboxLogger.Error().Err(err).Msg("failed to begin tx")
			time.Sleep(2 * time.Second)
			continue
		}

		records, err := outBox.FetchPending(ctx, tx, limit)
		if err != nil {
			_ = tx.Rollback(ctx)
			outboxLogger.Error().Err(err).Msg("failed to fetch outbox records")
			time.Sleep(2 * time.Second)
			continue
		}
		if len(records) == 0 {
			_ = tx.Rollback(ctx)
			time.Sleep(2 * time.Second)
			continue
		}
		publishedIDs := make([]uuid.UUID, 0, len(records))
		for _, record := range records {
			switch record.EventType {
			case EventTranscodeRequest:
				_, err := q.Publish(ctx, record.AggregateID.String())
				if err != nil {
					outboxLogger.Error().
						Err(err).
						Str("event_id", record.ID.String()).
						Msg("failed to publish outbox event")
					continue
				}
				publishedIDs = append(publishedIDs, record.ID)
			}
		}

		if len(publishedIDs) > 0 {
			err = outBox.MarkSent(ctx, tx, publishedIDs)
			if err != nil {
				_ = tx.Rollback(ctx)
				outboxLogger.Error().Err(err).Msg("failed to mark outbox sent")
				time.Sleep(2 * time.Second)
				continue
			}
		}

		err = tx.Commit(ctx)
		if err != nil {
			outboxLogger.Error().Err(err).Msg("failed to commit tx")
			time.Sleep(2 * time.Second)
			continue
		}
	}
}
