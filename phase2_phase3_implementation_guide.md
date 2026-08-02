# Phase 2 & Phase 3: Implementation Guide

**Date:** 2026-07-21  
**Purpose:** A hands-on, step-by-step guide to implement PostgreSQL persistence, the Transactional Outbox pattern, Redis Streams with consumer groups, retries, and dead-letter queues — while deeply learning Go's core concurrency primitives along the way.

---

## Where You Are Today

Your current codebase has:

| Component                     | Current State                                                           | What's Missing                                                                                                        |
| ----------------------------- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `internal/jobs/transcode.go`  | A bare `TranscodeJob` struct with only `VideoID` and `StorageSourceKey` | No `attempts`, `status`, `error_message`, `created_at` — no persistence at all                                        |
| `internal/queue/queue.go`     | `LPUSH` / `BLPOP` on a Redis List                                       | No acknowledgments. If a worker crashes mid-job, the message is lost forever. No retries, no DLQ                      |
| `service/video.go` `Upload()` | Generates a UUID, saves raw to MinIO, publishes to Redis, returns       | No database record. If Redis is down when you publish, the video is orphaned in MinIO with no record of its existence |
| `cmd/api/main.go`             | Wires everything up, serves HTTP                                        | No PostgreSQL connection. No background goroutines                                                                    |
| `cmd/worker/main.go`          | Worker pool with graceful shutdown                                      | No database updates. No retry logic. If `ProcessAndSaveHLS` fails, the job is silently dropped                        |

The core risk: **you have no source of truth**. If Redis loses a message, or a worker crashes, there is no record that a video was ever uploaded or that a job ever existed.

---

## Phase 2: PostgreSQL, Job Persistence & the Transactional Outbox

### Step 2.1 — Add PostgreSQL to Docker Compose

Add this to your `docker-compose.yml`:

```yaml
postgres:
    image: postgres:16-alpine
    container_name: postgres
    restart: always
    ports:
        - "5432:5432"
    environment:
        POSTGRES_USER: vod
        POSTGRES_PASSWORD: vod
        POSTGRES_DB: vod
    volumes:
        - pgdata:/var/lib/postgresql/data

# At the bottom of the file, add:
volumes:
    pgdata:
```

Add to your `Config` struct and `Load()`:

```go
DatabaseURL string  // e.g. "postgres://vod:vod@postgres:5432/vod?sslmode=disable"
```

> **🔍 Learn Deeply: Named Volumes vs Bind Mounts**  
> Your MinIO uses `~/minio-data:/data` (a bind mount). PostgreSQL above uses `pgdata:` (a named volume). Understand why named volumes are preferred for databases — Docker manages them, they survive `docker compose down`, and they avoid permission issues on Linux.

---

### Step 2.2 — Write SQL Migrations By Hand

Create a `migrations/` folder in your project root. Write raw SQL files — do NOT use an ORM.

**`migrations/000001_create_videos.up.sql`:**

```sql
CREATE TABLE IF NOT EXISTS videos (
    id              UUID PRIMARY KEY,
    title           TEXT NOT NULL DEFAULT '',
    original_filename TEXT NOT NULL DEFAULT '',
    size_bytes      BIGINT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'uploaded',
    storage_key     TEXT NOT NULL,
    playlist_key    TEXT NOT NULL DEFAULT '',
    playback_url    TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**`migrations/000002_create_transcode_jobs.up.sql`:**

```sql
CREATE TABLE IF NOT EXISTS transcode_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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
```

**`migrations/000003_create_outbox.up.sql`:**

```sql
CREATE TABLE IF NOT EXISTS outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id    UUID NOT NULL,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_pending ON outbox(created_at) WHERE status = 'pending';
```

> **🔍 Learn Deeply: `gen_random_uuid()` vs Application-Side UUIDs**  
> Right now your `service/video.go` generates UUIDs with `uuid.NewString()` in Go. That's fine for `videos.id` because you need the ID _before_ the INSERT (to construct the MinIO path). But for `transcode_jobs.id` and `outbox.id`, letting PostgreSQL generate UUIDs via `DEFAULT gen_random_uuid()` is cleaner — you don't need to know the job ID before inserting it.

> **🔍 Learn Deeply: `FOR UPDATE SKIP LOCKED`**  
> This is the most important SQL concept in this entire project. When the Outbox Publisher polls for pending rows, it uses `SELECT ... FOR UPDATE SKIP LOCKED`. This means: "Lock these rows so no other process can read them, but if another process already locked some rows, skip those instead of waiting." This is how you prevent two instances of the publisher from dispatching the same outbox event twice. Write a small test program that opens two concurrent `pgx` transactions and observe the locking behavior.

---

### Step 2.3 — Choose a PostgreSQL Driver and Learn Connection Pooling

Use `pgxpool` (from `github.com/jackc/pgx/v5/pgxpool`). This is the idiomatic, high-performance PostgreSQL driver for Go.

**Why `pgxpool` and not plain `pgx`?**

A single `pgx.Conn` is one TCP connection to PostgreSQL. If you have 5 concurrent HTTP requests, they would all fight over that one connection. `pgxpool.Pool` maintains a _pool_ of connections (e.g., 10) and lends one to each goroutine that needs it.

```go
// In internal/infra/infra.go, add:
func NewPostgresPool(cfg *config.Config) (*pgxpool.Pool, error) {
    poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
    if err != nil {
        return nil, fmt.Errorf("failed to parse database URL: %w", err)
    }
    poolConfig.MaxConns = 10
    poolConfig.MinConns = 2

    pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
    if err != nil {
        return nil, fmt.Errorf("failed to create connection pool: %w", err)
    }
    return pool, nil
}
```

> **🔍 Learn Deeply: Connection Pool Exhaustion**  
> Set `MaxConns = 3` temporarily. Then fire 10 concurrent upload requests. Watch what happens. Your requests will start queuing, waiting for a free connection. If any of your code forgets to release a connection (e.g., you acquire a transaction but never `Commit()` or `Rollback()`), the pool will permanently lose that connection. This is the most common production database bug. Understanding this will save you in every future project.

---

### Step 2.4 — Implement the Repository Layer

This is where you learn **clean code separation in Go**. Your current `service/video.go` directly calls MinIO and Redis. After this step, it will call a Repository interface, and the repository will talk to PostgreSQL.

**Create `internal/video/repository.go`:**

```go
package video

import (
    "context"
    "time"
    "github.com/google/uuid"
)

// The domain model — this is NOT a database row, it's a business object
type Video struct {
    ID               uuid.UUID
    Title            string
    OriginalFilename string
    SizeBytes        int64
    Status           string
    StorageKey       string
    PlaylistKey      string
    PlaybackURL      string
    CreatedAt        time.Time
    UpdatedAt        time.Time
}

// The interface — your service layer depends on THIS, not on pgx
type Repository interface {
    Create(ctx context.Context, v *Video) error
    UpdateStatus(ctx context.Context, id uuid.UUID, status string) error
    GetByID(ctx context.Context, id uuid.UUID) (*Video, error)
}
```

**Create `internal/video/postgres_repository.go`:**

```go
package video

import (
    "context"
    "fmt"
    "github.com/google/uuid"
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
        v.ID, v.Title, v.OriginalFilename, v.SizeBytes, v.Status, v.StorageKey, v.PlaylistKey, v.PlaybackURL,
    )
    if err != nil {
        return fmt.Errorf("failed to insert video: %w", err)
    }
    return nil
}
```

> **🔍 Learn Deeply: Why an Interface and Not Just the Struct?**  
> Your `VideoService` will depend on `video.Repository` (the interface), not `video.PostgresRepository` (the struct). This means:
>
> 1. In tests, you can swap in a fake/mock repository without needing a real database.
> 2. If you ever switch from PostgreSQL to CockroachDB, you only rewrite the repository implementation. The service layer doesn't change.
> 3. This is called **Dependency Inversion** — the most important design principle in Go.
>
> **Pay particular attention to:** Where you define the interface. In Go, interfaces are defined by the _consumer_, not the _provider_. The `service` package should ideally define the interface it needs, and the `internal/video` package implements it. This is the opposite of Java/C# where the implementation defines the interface.

---

### Step 2.5 — The Transactional Outbox (The Hardest and Most Valuable Part)

This is the crown jewel of Phase 2. It solves the **Dual-Write Problem**: you need to write to PostgreSQL AND publish to Redis atomically. If either fails independently, your system is inconsistent.

**The current broken flow in your `service/video.go` `Upload()`:**

```
1. Save raw video to MinIO           ← Can fail
2. v.Queue.Publish(job)              ← Can fail independently
3. Return success to client
```

If step 2 fails (Redis is down), the video sits in MinIO forever with no job to process it. There is no record it exists.

**The correct flow after Phase 2:**

```
1. Save raw video to MinIO
2. BEGIN TRANSACTION
   a. INSERT INTO videos (...)
   b. INSERT INTO transcode_jobs (...)
   c. INSERT INTO outbox (event_type='transcode.requested', payload=...)
   COMMIT
3. Return 202 Accepted to client
```

Now a **separate background goroutine** (the Outbox Publisher) polls the outbox table and publishes to Redis:

**Create `internal/outbox/publisher.go`:**

```go
package outbox

import (
    "context"
    "time"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/rs/zerolog"
)

type Publisher struct {
    pool   *pgxpool.Pool
    queue  queue.Queue  // Your existing Queue interface
    logger zerolog.Logger
}

func (p *Publisher) Run(ctx context.Context) {
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            p.logger.Info().Msg("outbox publisher shutting down")
            return
        case <-ticker.C:
            p.publishPending(ctx)
        }
    }
}

func (p *Publisher) publishPending(ctx context.Context) {
    // 1. BEGIN transaction
    // 2. SELECT * FROM outbox WHERE status = 'pending'
    //    ORDER BY created_at LIMIT 10
    //    FOR UPDATE SKIP LOCKED
    // 3. For each row: publish to Redis queue
    // 4. UPDATE outbox SET status = 'sent' WHERE id IN (...)
    // 5. COMMIT
}
```

> **🔍 Learn Deeply: The `select` Statement**  
> The `select` block in `Run()` is one of the most important Go concurrency primitives. It blocks until ONE of the cases is ready:
>
> - `<-ctx.Done()` fires when the application receives SIGTERM
> - `<-ticker.C` fires every 500ms
>
> This is fundamentally different from a `for` loop with `time.Sleep()`. With `select`, cancellation is instantaneous — the goroutine exits the moment the context is cancelled, even if it was in the middle of waiting for the next tick. With `time.Sleep()`, the goroutine would sleep for the full duration before checking if it should stop.
>
> **Exercise:** Write a small standalone program with two goroutines communicating via channels and a `select` statement. Make one goroutine a producer (sends numbers on a channel) and the other a consumer that uses `select` to read from the channel OR exit on `ctx.Done()`. Run it, then cancel the context after 3 seconds. Observe the behavior.

> **🔍 Learn Deeply: Database Transactions in Go (`pgx.Tx`)**  
> The three INSERTs (video, job, outbox) MUST be inside a single transaction. Here is the pattern:
>
> ```go
> tx, err := pool.Begin(ctx)
> if err != nil { return err }
> defer tx.Rollback(ctx) // Always rollback if we haven't committed
>
> // ... do all three INSERTs using tx.Exec() ...
>
> return tx.Commit(ctx) // Only commits if we reach here
> ```
>
> The `defer tx.Rollback(ctx)` is the safety net. If ANY error occurs and the function returns early, the deferred Rollback fires and undoes everything. If `Commit()` succeeds, the deferred `Rollback()` is a no-op. This is an elegant Go pattern — understand it deeply because you will use it everywhere.

---

### Step 2.6 — Wire the Outbox Publisher into `cmd/api/main.go`

The Outbox Publisher runs as a background goroutine inside your API process. This is where you practice launching and cleanly shutting down concurrent goroutines.

```go
func main() {
    // ... existing setup ...

    // Create the outbox publisher
    outboxPublisher := outbox.NewPublisher(pool, transcodeQueue, outboxLogger)

    // Launch it in a background goroutine
    // It will run until ctx is cancelled (SIGTERM)
    go outboxPublisher.Run(ctx)

    // ... existing HTTP server setup ...
    // ... existing shutdown logic ...

    // After server.Shutdown(), the outbox publisher is also stopped
    // because they share the same ctx
}
```

> **🔍 Learn Deeply: Goroutine Lifecycle Management**  
> Your API process now has TWO concurrent things running: the HTTP server and the Outbox Publisher. Both need to shut down cleanly when SIGTERM arrives. Right now your `cmd/api/main.go` uses a manual `signal.Notify` + channel pattern for the HTTP server. Consider how the outbox publisher goroutine fits into this lifecycle. Does it need its own `sync.WaitGroup`? What happens if the publisher is in the middle of a database transaction when shutdown fires? Think through these edge cases.

### Final architecture

                videos
          ┌──────────────────┐
          │ id               │
          │ upload_status    │
          │ transcode_status │
          │ storage_key      │
          └─────────┬────────┘
                    │
                    │ 1:N
                    ▼
          transcode_jobs
          ┌──────────────────┐
          │ id               │
          │ video_id         │
          │ status           │
          │ attempts         │
          │ locked_by        │
          └─────────┬────────┘
                    │
                    │ transaction
                    ▼
               outbox
          ┌──────────────────┐
          │ id               │
          │ event_type       │
          │ aggregate_id     │
          │ payload          │
          │ published        │
          └─────────┬────────┘
                    │
                    ▼
                 Redis
                    │
                    ▼
                 Worker

---

## Phase 3: Redis Streams, Retries & Dead-Letter Queues

### Why Your Current Queue is Broken

Your current `queue.go` uses `LPUSH` / `BLPOP`. Here is the fatal flaw:

```
1. Worker calls BLPOP → Redis removes the message from the list and hands it to the worker
2. Worker starts processing
3. Worker crashes (OOM, power failure, Docker kill)
4. The message is GONE. Redis already deleted it. Nobody will ever process that video.
```

This is called **at-most-once delivery**. You need **at-least-once delivery**: Redis keeps the message until the worker explicitly says "I'm done, you can delete it now."

---

### Step 3.1 — Upgrade to Redis Streams

Redis Streams are fundamentally different from Redis Lists. A Stream is an append-only log (like Kafka) with **Consumer Groups**.

**Replace your `internal/queue/queue.go`:**

The new interface should look like:

```go
type Queue interface {
    Publish(ctx context.Context, job jobs.TranscodeJob) error
    Consume(ctx context.Context) (*jobs.TranscodeJob, string, error) // returns messageID too
    Ack(ctx context.Context, messageID string) error                 // NEW: acknowledge completion
}
```

Key Redis Stream commands you will use:

```
XADD   transcode_queue * payload "{...}"              — Publish (append to stream)
XREADGROUP GROUP workers consumer1 BLOCK 0 COUNT 1 STREAMS transcode_queue >  — Consume (claim a message)
XACK   transcode_queue workers <message-id>            — Acknowledge (mark as processed)
```

> **🔍 Learn Deeply: Consumer Groups**  
> A Consumer Group is a named cursor on a Redis Stream. When Worker A reads a message, that message is assigned to Worker A in a **Pending Entries List (PEL)**. If Worker A crashes without calling `XACK`, the message stays in the PEL. Another worker can later **claim** it using `XAUTOCLAIM`.
>
> **Exercise:** Open `redis-cli` inside your Redis container and manually run these commands:
>
> ```
> XADD mystream * msg "hello"
> XGROUP CREATE mystream mygroup 0 MKSTREAM
> XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >
> XPENDING mystream mygroup                   # See the unclaimed message
> XACK mystream mygroup <message-id>          # Now it's gone from PEL
> ```
>
> Do this by hand before writing any Go code. Understanding the PEL is critical.

---

### Step 3.2 — Implement the Ack Pattern in Your Worker

Your worker loop changes from "consume and forget" to "consume, process, then acknowledge":

```go
for {
    job, messageID, err := v.Queue.Consume(ctx)
    if err != nil { ... }

    // Process the job
    err = v.ProcessAndSaveHLS(jobCtx, job.VideoID, job.StorageSourceKey, output)

    if err != nil {
        // DON'T ack — the message stays in the PEL
        // Update the database: increment attempts, set error_message
        // If attempts >= max_attempts, move to DLQ
        handleFailure(ctx, job, messageID, err)
    } else {
        // Success: acknowledge the message (removes from PEL)
        v.Queue.Ack(ctx, messageID)
        // Update the database: set status = 'completed'
    }
}
```

> **🔍 Learn Deeply: The Difference Between Ack and No-Ack**  
> If your worker crashes between `Consume()` and `Ack()`, the message is NOT lost. It sits in the Pending Entries List. When the worker restarts (or another worker runs `XAUTOCLAIM`), the message is redelivered. This is **at-least-once delivery**. The tradeoff: your `ProcessAndSaveHLS` must be **idempotent** — running it twice on the same video must produce the same result without corruption. Think about what happens if you upload segments to MinIO twice. Is that safe? (Hint: yes, because MinIO overwrites objects with the same key.)

---

### Step 3.3 — Implement Retry Logic with Exponential Backoff

When a job fails, don't immediately retry it. The failure might be because MinIO is temporarily overloaded. Hammering it again instantly makes things worse.

```go
func calculateBackoff(attempts int) time.Duration {
    base := time.Duration(1<<uint(attempts)) * time.Second  // 1s, 2s, 4s, 8s, 16s...
    jitter := time.Duration(rand.Intn(500)) * time.Millisecond
    maxBackoff := 5 * time.Minute
    if base > maxBackoff {
        base = maxBackoff
    }
    return base + jitter
}
```

> **🔍 Learn Deeply: Why Jitter Matters**  
> Without jitter, if 10 workers all fail at the same time (because MinIO went down for 2 seconds), they all retry at exactly the same moment — causing a "thundering herd" that overwhelms MinIO again. Jitter adds randomness so retries are spread out. This is a real production concern, not theoretical.

---

### Step 3.4 — Implement the Error Classifier

Not all errors deserve a retry. A corrupt video file will fail every single time — retrying it 3 times wastes 30 minutes of CPU.

**Create `internal/transcoder/errors.go`:**

```go
package transcoder

import "strings"

type ErrorSeverity int

const (
    SeverityTransient ErrorSeverity = iota  // Retry: network timeout, disk full, MinIO down
    SeverityPermanent                        // Don't retry: corrupt file, unsupported codec
)

func ClassifyError(err error, stderrOutput string) ErrorSeverity {
    permanentPatterns := []string{
        "Invalid data found when processing input",
        "Decoder not found",
        "Invalid argument",
        "No such file or directory",
    }
    for _, pattern := range permanentPatterns {
        if strings.Contains(stderrOutput, pattern) {
            return SeverityPermanent
        }
    }
    return SeverityTransient
}
```

> **🔍 Learn Deeply: Error Wrapping and Unwrapping**  
> Right now your code uses `fmt.Errorf("transcode failed: %w", err)` everywhere. The `%w` verb _wraps_ the original error. In your worker, you can use `errors.Is(err, context.DeadlineExceeded)` to check if the failure was a timeout, or `errors.As(err, &targetType)` to extract a custom error type. These are the three pillars of Go error handling: `fmt.Errorf` with `%w`, `errors.Is()`, and `errors.As()`. Master all three.
>
> **Exercise:** Define a custom `TranscodeError` type that carries the `stderrOutput` string. Wrap it with `%w` in `ffmpeg.go`. In the worker, use `errors.As()` to extract the stderr and pass it to `ClassifyError()`.

---

### Step 3.5 — Implement the Dead-Letter Queue

When `attempts >= max_attempts`, the message is moved to a separate Redis Stream called `transcode_queue.dlq`. It sits there permanently for manual inspection.

```go
func (rq *RedisStreamQueue) MoveToDLQ(ctx context.Context, messageID string, job jobs.TranscodeJob, reason string) error {
    // 1. XADD transcode_queue.dlq * payload "{...}" error "..." attempts "3"
    // 2. XACK transcode_queue workers <messageID>  (remove from primary stream PEL)
}
```

> **🔍 Learn Deeply: DLQ Inspection and Replay**  
> A DLQ is useless if you can't inspect it. Write a small CLI tool (or just a function) that:
>
> 1. `XRANGE transcode_queue.dlq - +` — Lists all dead-lettered messages
> 2. Allows you to manually re-publish a message back to the primary queue after you've fixed the root cause
>
> This is a common production operations task. SREs (Site Reliability Engineers) do this regularly.

---

## Implementation Order (Do It In This Exact Sequence)

| Step | What to Build                                                                                                                                           | Go Concepts You Will Learn                                                                  |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| 1    | Add PostgreSQL to docker-compose, write migration SQL files, run them manually with `psql`                                                              | SQL, Docker networking                                                                      |
| 2    | Create `NewPostgresPool()` in `internal/infra/infra.go`                                                                                                 | `pgxpool`, connection pooling, `context.Context` propagation                                |
| 3    | Create `internal/video/repository.go` (interface) and `postgres_repository.go` (implementation)                                                         | Interface design, dependency inversion, `pgx` row scanning                                  |
| 4    | Similarly create `internal/transcode/repository.go` for the jobs table                                                                                  | Same patterns, reinforcement                                                                |
| 5    | Modify `service/video.go` `Upload()` to use a database transaction: insert video + job + outbox in one `pgx.Tx`                                         | **Transactions**, `defer tx.Rollback()`, atomicity                                          |
| 6    | Build `internal/outbox/publisher.go` with `Run(ctx)` using `select` + `time.Ticker`                                                                     | **`select` statement**, **goroutine lifecycle**, `context.Done()`, `FOR UPDATE SKIP LOCKED` |
| 7    | Wire the outbox publisher into `cmd/api/main.go` as a background goroutine                                                                              | Goroutine management, graceful shutdown coordination                                        |
| 8    | **Stop here. Test everything end-to-end.** Upload a video via API, verify rows appear in all 3 tables, verify the outbox publisher dispatches to Redis. | Integration testing, `docker exec -it postgres psql`                                        |
| 9    | Replace `internal/queue/queue.go` LPUSH/BLPOP with Redis Streams (XADD/XREADGROUP/XACK)                                                                 | Redis Streams, consumer groups, PEL                                                         |
| 10   | Add `Ack()` to the worker loop in `cmd/worker/main.go`                                                                                                  | At-least-once delivery, idempotency                                                         |
| 11   | Create `internal/transcoder/errors.go` (error classifier)                                                                                               | Custom error types, `errors.Is()`, `errors.As()`, `%w` wrapping                             |
| 12   | Implement retry logic with exponential backoff + jitter                                                                                                 | `time.Duration` arithmetic, `math/rand`                                                     |
| 13   | Implement the DLQ stream                                                                                                                                | Redis XADD to a secondary stream, XACK on primary                                           |
| 14   | Update the worker to call the transcode repository: set `status='processing'` when starting, `status='completed'` or `status='failed'` when done        | Database writes from the worker, same `pgxpool` patterns                                    |

---

## Things to Pay Particular Attention To

### 1. Context Propagation (The #1 Go Skill)

Your current code already uses contexts, but inconsistently. In `service/video.go` `Upload()`, you create `context.WithTimeout(context.Background(), 10*time.Minute)` for the MinIO upload. But the incoming HTTP request _already has a context_ (`r.Context()`). If the client disconnects, `r.Context()` is cancelled — but your `context.Background()` timeout keeps running for 10 minutes uploading a file nobody wants anymore.

**Rule of thumb:** Never use `context.Background()` inside request handlers. Always derive from the request context. The only place `context.Background()` is appropriate is in the worker's job processing (because the job should finish even if the worker's parent context is cancelled — which you already do correctly).

### 2. The Repository Pattern Is Not About "Clean Architecture" Buzzwords

It's about **testability**. When you have `video.Repository` as an interface, you can write a test for `VideoService.Upload()` that uses a fake in-memory repository. You don't need Docker, PostgreSQL, or MinIO running. You just verify that `Upload()` calls `repo.Create()` with the correct arguments. This is practical, not theoretical.

### 3. Don't Over-Abstract Too Early

You do NOT need:

- A generic `Repository[T]` with generics
- A "Unit of Work" pattern
- A "Domain Event Bus"

Just write concrete repository structs with concrete methods. If you find yourself repeating code later, refactor then.

### 4. Capture FFmpeg stderr

Right now your `internal/transcoder/ffmpeg.go` discards FFmpeg's stderr output. When you build the error classifier, you need that output. Capture it into a `bytes.Buffer`:

```go
var stderr bytes.Buffer
cmd.Stderr = &stderr
err := cmd.Run()
// Now stderr.String() contains FFmpeg's diagnostic output
```

This string is what you pass to `ClassifyError()`.

### 5. Idempotency

Once you move to at-least-once delivery (Redis Streams), a job _might_ be delivered twice (if the worker crashed after processing but before acking). Your `ProcessAndSaveHLS` must handle this gracefully. Since MinIO overwrites objects with the same key, re-uploading segments is safe. But if you later add a database status update, make sure updating `status = 'completed'` twice doesn't cause an error or duplicate side effects.

---

## What You Should NOT Do Yet

| Don't do this                          | Why                                                                                                    |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Add Prometheus metrics                 | You don't have enough data flowing through the system yet. Add metrics after Phase 3 is working.       |
| Add the ALG stack (Alloy/Loki/Grafana) | Your zerolog JSON logs are already captured by Docker. Add Loki after you have real failures to debug. |
| Add gRPC                               | You don't have multiple services that need to talk to each other via RPC yet.                          |
| Add WebSockets                         | You need the database and outbox working first to have progress events worth pushing.                  |
| Add Circuit Breakers                   | Premature. Get the happy path and basic retry path working first.                                      |

---

## Suggested Reading While Implementing

- **Go's `context` package documentation** — Read the entire thing: https://pkg.go.dev/context. It is short and every word matters.
- **pgx documentation** — Specifically the sections on transactions and connection pools: https://github.com/jackc/pgx
- **Redis Streams introduction** — The official Redis documentation is excellent: https://redis.io/docs/data-types/streams-tutorial/
- **"Distributed Services with Go" (Travis Jeffery)** — The log/index chapters (1-4) are gold for understanding file I/O and concurrent access. Don't worry about the protobuf chapters yet — come back to them after you finish Phase 3 here.
