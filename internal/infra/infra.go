package infra

import (
	"fmt"

	"github.com/arjun118/fileupload/internal/config"
	miniosdk "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
)

func NewMinioClient(cfg *config.Config) (*miniosdk.Client, error) {
	client, err := miniosdk.New(cfg.MinioEndpoint, &miniosdk.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioUseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}
	return client, nil
}

func NewRedisClient(cfg *config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
}
