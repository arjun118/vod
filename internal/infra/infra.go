package infra

import (
	"context"
	"fmt"
	"time"

	"github.com/arjun118/fileupload/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
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

func NewPostgresPool(cfg *config.Config) (*pgxpool.Pool, error) {
	ctx := context.Background()

	// 1. Parse the connection string into a pgxpool.Config struct
	poolConfig, err := pgxpool.ParseConfig(cfg.DB.DATABASE_URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database url: %w", err)
	}

	// 2. Configure Max Open Connections (MaxConns)
	poolConfig.MaxConns = int32(cfg.DB.MaxOpenConns)

	// 3. Configure Min Idle Connections (Optional, akin to MaxIdleConns)
	// pgxpool manages idle connections via MinConns. Set it if you want pre-allocated idle connections.
	poolConfig.MinConns = int32(cfg.DB.MaxIdleConns)

	// 4. Configure Max Connection Idle Time
	// Equivalent to db.SetConnMaxIdleTime()
	maxIdleDuration, err := time.ParseDuration(cfg.DB.MaxIdleTime)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max idle time: %w", err)
	}
	poolConfig.MaxConnIdleTime = maxIdleDuration

	// 5. Initialize the pool using the customized config
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database pool: %w", err)
	}

	// 6. Verify the connection with a ping/timeout
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}
