create table if not exists outbox (
    id uuid primary key default gen_random_uuid(),
    aggregate_id uuid not null,
    event_type text not null,
    payload jsonb not null,
    status text not null default 'pending',
    created_at timestamptz(0) not null default now()
);

CREATE INDEX idx_outbox_pending ON outbox(created_at) WHERE status = 'pending';
