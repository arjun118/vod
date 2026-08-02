package video

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Create(ctx context.Context, v *Video) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO videos (id, title, original_filename, size_bytes, status, storage_key, playlist_key, playback_url)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		v.ID, v.Title, v.OriginalFilename, v.SizeBytes, v.UploadStatus, v.StorageKey, v.PlaylistKey, v.PlaybackURL,
	)
	if err != nil {
		return fmt.Errorf("failed to insert video: %w", err)
	}
	return nil
}
