package transcode

import (
	"context"
	"errors"
	"fmt"

	"github.com/arjun118/fileupload/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrJobNotFound     = errors.New("transcode job not found")
	ErrInvalidJobState = errors.New("invalid transcode job state")
)

type Repository struct{}

func NewRepository() *Repository {
	return &Repository{}
}

// Create inserts a new transcode job.
func (r *Repository) Create(
	ctx context.Context,
	db database.DBTX,
	job *Job,
) error {

	const query = `
INSERT INTO transcode_jobs (
	video_id,
	max_attempts
)
VALUES (
	$1,
	$2
)
RETURNING id;
`

	err := db.QueryRow(
		ctx,
		query,
		job.VideoID,
		job.MaxAttempts,
	).Scan(&job.ID)

	if err != nil {
		return fmt.Errorf("create transcode job: %w", err)
	}

	return nil
}

// GetByVideoID fetches the job associated with a video.
func (r *Repository) GetByVideoID(
	ctx context.Context,
	db database.DBTX,
	videoID uuid.UUID,
) (*Job, error) {

	var job Job

	query := `
SELECT
` + jobColumns + `
FROM transcode_jobs
WHERE video_id = $1;
`

	err := db.QueryRow(ctx, query, videoID).Scan(
		&job.ID,
		&job.VideoID,
		&job.Status,
		&job.Attempts,
		&job.MaxAttempts,
		&job.ErrorMessage,
		&job.ProcessedBy,
		&job.StartedAt,
		&job.FinishedAt,
		&job.CreatedAt,
		&job.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrJobNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("get transcode job: %w", err)
	}

	return &job, nil
}

// MarkProcessing atomically claims the job for processing.
// This performs optimistic locking and increments the attempt counter.
func (r *Repository) MarkProcessing(
	ctx context.Context,
	db database.DBTX,
	jobID uuid.UUID,
	workerID string,
) error {

	const query = `
UPDATE transcode_jobs
SET
	status = $1,
	attempts = attempts + 1,
	processed_by = $2,
	started_at = NOW(),
	updated_at = NOW()
WHERE
	id = $3
	AND status = $4;
`

	tag, err := db.Exec(
		ctx,
		query,
		StatusProcessing,
		workerID,
		jobID,
		StatusPending,
	)

	if err != nil {
		return fmt.Errorf("mark processing: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrInvalidJobState
	}

	return nil
}

// Update persists the current state of the Job.
// MarkProcessing is intentionally separate because it requires
// an atomic increment of attempts.
func (r *Repository) Update(
	ctx context.Context,
	db database.DBTX,
	job *Job,
) error {

	const query = `
UPDATE transcode_jobs
SET
	status = $1,
	error_message = $2,
	processed_by = $3,
	started_at = $4,
	finished_at = $5,
	updated_at = NOW()
WHERE
	id = $6;
`

	tag, err := db.Exec(
		ctx,
		query,
		job.Status,
		job.ErrorMessage,
		job.ProcessedBy,
		job.StartedAt,
		job.FinishedAt,
		job.ID,
	)

	if err != nil {
		return fmt.Errorf("update transcode job: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrJobNotFound
	}

	return nil
}

func (r *Repository) GetExecutionInfo(
	ctx context.Context,
	db database.DBTX,
	jobID string,
) (*ExecutionInfo, error) {
	var execInfo ExecutionInfo
	const query = `
	select t.job_id,v.id, storage_key, playlist_key, t.status, t.attempts, t.max_attempts
	from videos v
	inner join ( select id as job_id, video_id, status,attempts, max_attempts from transcode_jobs
	where id=$1
	) t
	on v.id= t.video_id
	`

	// SourceKey   string
	// PlaylistKey string
	err := db.QueryRow(ctx, query, jobID).Scan(
		&execInfo.Job.ID,
		&execInfo.Job.VideoID,
		&execInfo.SourceKey,
		&execInfo.PlaylistKey,
		&execInfo.Job.Status,
		&execInfo.Job.Attempts,
		&execInfo.Job.MaxAttempts,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get job execution info: %w", err)
	}
	return &execInfo, nil
}
