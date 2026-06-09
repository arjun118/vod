package media

import (
	"context"
	"io"
)

type FileMetaData struct {
	Filename    string
	ContentType string
	Size        int64
	OwnerID     string
}

type MediaObject struct {
	ID           string `json:"id"`
	ObjectKey    string `json:"key"`
	Provider     string `json:"provider"`
	ContentType  string `json:"content_type"`
	Size         int64  `json:"size"`
	PlaybackURL  string `json:"playback_url"`
	ThumbnailURL string `json:"thumbnail_url"`
}

type StorageProvider interface {
	Save(ctx context.Context, objectKey string, r io.Reader, meta FileMetaData) (int64, error)
	Delete(ctx context.Context, objectKey string) error
}

type DeliveryProvider interface {
	URL(ctx context.Context, objectKey string) (string, error)
}
