package delivery

import (
	"context"
	"fmt"
	"strings"
)

type LocalDelivery struct {
	BaseURL string
}

func NewLocalDelivery(baseURL string) *LocalDelivery {
	return &LocalDelivery{
		BaseURL: baseURL,
	}
}

func (d *LocalDelivery) URL(ctx context.Context, objectKey string) (string, error) {
	return d.playbackURL(objectKey), nil
}

func (d *LocalDelivery) playbackURL(objectKey string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(d.BaseURL, "/"), objectKey)
}
