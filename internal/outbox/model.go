package outbox

import (
	"time"

	"github.com/google/uuid"
)

type EventStatus string

const (
	StatusPending EventStatus = "pending"
	StatusSent    EventStatus = "sent"
)

type Type string

const (
	EventTranscodeRequest Type = "transcode.request"
)

type Event struct {
	ID          uuid.UUID
	AggregateID uuid.UUID //for video transcode this will be out jobid
	EventType   Type
	Payload     []byte //jsonb is stored as raw bytes
	Status      EventStatus
	CreatedAt   time.Time
}
