package outbox

import (
	"context"
	"fmt"

	"github.com/arjun118/fileupload/internal/database"
	"github.com/google/uuid"
)

// create table if not exists outbox (
//     id uuid primary key default gen_random_uuid(),
//     aggregate_id uuid not null,
//     event_type text not null,
//     payload jsonb not null,
//     status text not null default 'pending',
//     created_at timestamptz(0) not null default now()
// );

type Repository struct {
}

func NewRepository() *Repository {
	return &Repository{}
}

// CreateTx inserts an outbox event within an existing transaction.
func (r *Repository) Create(ctx context.Context, db database.DBTX, event *Event) error {
	const query = `
	INSERT INTO outbox (aggregate_id, event_type, payload)
	values ($1,$2,$3)
	returning id
	`
	args := []any{event.AggregateID, event.EventType, event.Payload}
	err := db.QueryRow(ctx, query, args...).Scan(&event.ID)
	if err != nil {
		return fmt.Errorf("failed create outbox entry: %w", err)
	}
	return nil
}

// FetchPending retrieves up to `limit` pending events, locking them
// with FOR UPDATE SKIP LOCKED to prevent concurrent publishers
// from grabbing the same rows.
// Returns the events AND the pgx.Tx (caller must Commit or Rollback).
func (r *Repository) FetchPending(
	ctx context.Context,
	db database.DBTX,
	limit int,
) ([]*Event, error) {

	const query = `
SELECT
	id,
	aggregate_id,
	event_type,
	payload,
	status,
	created_at
FROM outbox
WHERE status = 'pending'
ORDER BY created_at
LIMIT $1
FOR UPDATE SKIP LOCKED;
`

	rows, err := db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch pending events: %w", err)
	}
	defer rows.Close()

	var events []*Event

	for rows.Next() {

		var e Event

		err := rows.Scan(
			&e.ID,
			&e.AggregateID,
			&e.EventType,
			&e.Payload,
			&e.Status,
			&e.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}

		events = append(events, &e)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

// MarkSent updates the status of the given event IDs to 'sent'.
// Must be called within the same transaction returned by FetchPending.
func (r *Repository) MarkSent(ctx context.Context, db database.DBTX, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	const query = `
	UPDATE outbox
	SET status = $1
	WHERE id = ANY($2);
	`

	tag, err := db.Exec(
		ctx,
		query,
		EventSent,
		ids,
	)

	if err != nil {
		return fmt.Errorf("mark outbox sent: %w", err)
	}

	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("expected to update %d rows, updated %d",
			len(ids),
			tag.RowsAffected(),
		)
	}

	return nil
}
