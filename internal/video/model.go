package video

import (
	"time"

	"github.com/google/uuid"
)

type Video struct {
	ID               uuid.UUID
	Title            string
	OriginalFilename string
	SizeBytes        int64
	StorageKey       string
	UploadStatus     string
	PlaylistKey      string
	PlaybackURL      string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}
