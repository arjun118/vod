CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS videos (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    title text NOT NULL DEFAULT '',
    original_filename text NOT NULL DEFAULT '',
    size_bytes bigint NOT NULL DEFAULT 0,
    storage_key text NOT NULL,
    upload_status TEXT NOT NULL DEFAULT 'pending',
    playlist_key text NOT NULL DEFAULT '',
    playback_url text NOT NULL DEFAULT '',
    created_at timestamptz(0) NOT NULL DEFAULT NOW(),
    updated_at timestamptz(0) NOT NULL DEFAULT NOW()
);
