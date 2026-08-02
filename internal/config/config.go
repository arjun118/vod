package config

import (
	"os"
	"strconv"
)

type Config struct {
	MinioEndpoint         string
	MinioAccessKey        string
	MinioSecretKey        string
	MinioUseSSL           bool
	RawBucketName         string
	StreamsBucketName     string
	RedisAddr             string
	DatabaseURL           string
	NginxDeliveryEndpoint string
	MaxTranscodeWorkers   int
	TranscodeQueueName    string
}

func Load() *Config {
	return &Config{
		MinioEndpoint:         getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKey:        getEnv("MINIO_ACCESS_KEY", "adminpass"),
		MinioSecretKey:        getEnv("MINIO_SECRET_KEY", "adminpass"),
		MinioUseSSL:           getEnv("MINIO_USE_SSL", "false") == "true",
		RawBucketName:         getEnv("MINIO_RAW_BUCKET", "uploads"),
		StreamsBucketName:     getEnv("MINIO_STREAMS_BUCKET", "streams"),
		RedisAddr:             getEnv("REDIS_ADDR", "localhost:6379"),
		NginxDeliveryEndpoint: getEnv("NGINX_DELIVERY_ENDPOINT", "localhost:8080/media"),
		MaxTranscodeWorkers:   getEnvInt("MAX_TRANSCODE_WORKERS", 5),
		TranscodeQueueName:    getEnv("TRANSCODE_QUEUE_NAME", "transcode_queue"),
		DatabaseURL:           getEnv("DATABASE_URL", "postgres://vod:vod@postgres:5432/vod?sslmode=disable"),
	}
}

func getEnvInt(key string, fallback int) int {
	if value, exists := os.LookupEnv(key); exists {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return fallback
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
