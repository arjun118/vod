create table if not exists transcode_jobs (
    id uuid primary key default gen_random_uuid(),
    video_id        UUID NOT NULL REFERENCES videos(id),
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 3,
    error_message   TEXT,
    locked_by       TEXT,
    locked_at       TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transcode_jobs_status_created
ON transcode_jobs(status, created_at);
