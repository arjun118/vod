package delivery

import (
	"context"
	"fmt"
	"strings"
)

type Delivery struct {
	BaseURL string
}

func NewDelivery(baseURL string) *Delivery {
	return &Delivery{
		BaseURL: baseURL,
	}
}

func (d *Delivery) URL(ctx context.Context, objectKey string) (string, error) {
	return d.playbackURL(objectKey), nil
}

func (d *Delivery) playbackURL(objectKey string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(d.BaseURL, "/"), objectKey)
}
