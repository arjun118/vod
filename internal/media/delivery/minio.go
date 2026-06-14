package delivery

import (
	"context"
	"fmt"
	"log"
	"strings"
)

type MinioDelivery struct {
	BucketName string
	EndPoint   string
}

func NewMinioDelivery(bucketName string, endpoint string) *MinioDelivery {
	return &MinioDelivery{
		BucketName: bucketName,
		EndPoint:   endpoint,
	}
}

func (m *MinioDelivery) URL(ctx context.Context, objectKey string) (string, error) {
	return m.playbackURL(objectKey), nil
}

func (m *MinioDelivery) playbackURL(objectKey string) string {
	url := fmt.Sprintf("http://%s/%s", strings.TrimRight(m.EndPoint, "/"), objectKey)
	log.Println("playback url called with objectkey: ", objectKey, "url: ", url)
	return url
}
