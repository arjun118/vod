package outbox

import (
	"time"

	"github.com/google/uuid"
)

type EventStatus string

const (
	EventPending EventStatus = "pending"
	EventSent    EventStatus = "sent"
)

type Event struct {
	ID          uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Payload     []byte //jsonb is stored as raw bytes
	Status      EventStatus
	CreatedAt   time.Time
}
