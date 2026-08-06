package transcode

import (
	"time"

	"github.com/google/uuid"
)

type TranscodeJob struct {
	VideoID string `json:"videoid"`

	// Object key of the uploaded source video.
	StorageSourceKey string `json:"source_key"`

	// Destination playlist key.
	PlaylistKey string `json:"playlist_key"`
}

const jobColumns = `
id,
video_id,
status,
attempts,
max_attempts,
error_message,
processed_by,
started_at,
finished_at,
created_at,
updated_at
`

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Job struct {
	ID          uuid.UUID
	VideoID     uuid.UUID
	Status      Status
	Attempts    int
	MaxAttempts int

	ErrorMessage *string
	ProcessedBy  *string

	StartedAt  *time.Time
	FinishedAt *time.Time
	CreatedAt  *time.Time
	UpdatedAt  *time.Time
}

func (j Job) CanRetry() bool {
	return j.Attempts < j.MaxAttempts
}

func (j Job) IsPending() bool {
	return j.Status == StatusPending
}

func (j Job) IsProcessing() bool {
	return j.Status == StatusProcessing
}

func (j Job) IsCompleted() bool {
	return j.Status == StatusCompleted
}

func (j Job) IsFailed() bool {
	return j.Status == StatusFailed
}

func (j *Job) MarkCompleted() {
	now := time.Now()

	j.Status = StatusCompleted
	j.FinishedAt = &now
	j.UpdatedAt = &now
	j.ErrorMessage = nil
}

func (j *Job) MarkFailed(err error) {
	now := time.Now()

	j.Status = StatusFailed
	j.FinishedAt = &now
	j.UpdatedAt = &now

	if err != nil {
		msg := err.Error()
		j.ErrorMessage = &msg
	}
}

func (j *Job) ResetForRetry() {
	now := time.Now()

	j.Status = StatusPending
	j.UpdatedAt = &now
	j.ErrorMessage = nil
}
