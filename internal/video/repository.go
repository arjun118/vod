package video

import (
	"context"
	"fmt"

	"github.com/arjun118/fileupload/internal/database"
	"github.com/google/uuid"
)

type Repository struct{}

func NewRepository() *Repository {
	return &Repository{}
}

func (r *Repository) Create(
	ctx context.Context,
	db database.DBTX,
	v *Video,
) error {
	sqlString := `
	INSERT INTO videos (title, original_filename,size_bytes, storage_key, playlist_key, playback_url) values
	($1,$2,$3,$4,$5,$6) returning id, created_at,updated_at
	`
	args := []any{v.Title, v.OriginalFilename, v.SizeBytes, v.StorageKey, v.PlaylistKey, v.PlaybackURL}
	err := db.QueryRow(ctx, sqlString, args...).Scan(&v.ID, &v.CreatedAt, &v.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to create record for video: %w", err)
	}
	return nil
}

func (r *Repository) UpdateStatus(
	ctx context.Context,
	db database.DBTX,
	id uuid.UUID,
	status string,
) error {
	sqlString := `
	UPDATE videos
	SET upload_status= $1
	WHERE id=$2;
	`
	args := []any{status, id}
	err := db.QueryRow(ctx, sqlString, args...)
	if err != nil {
		return fmt.Errorf("failed to update upload status : %w", err)
	}
	return nil
}

func (r *Repository) GetByID(
	ctx context.Context,
	db database.DBTX,
	id uuid.UUID,
) (*Video, error) {
	var vid Video
	sqlString := `
	select id,title, original_filename,
	size_bytes, storage_key, upload_status, playlist_key, playback_url,
	created_at,updated_at
	from videos
	where id=$1
	`
	args := []any{id}
	err := db.QueryRow(ctx, sqlString, args...).Scan(
		&vid.ID,
		&vid.Title,
		&vid.OriginalFilename,
		&vid.SizeBytes,
		&vid.StorageKey,
		&vid.UploadStatus,
		&vid.PlaylistKey,
		&vid.PlaybackURL,
		&vid.CreatedAt,
		&vid.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get video details: %w", err)
	}
	return &vid, nil
}

func (r *Repository) List(
	ctx context.Context,
	db database.DBTX,
) ([]*Video, error) {
	panic("not implemented")
}
