# Codebase Architecture Critique & System Flow
**Reviewer:** Senior Go Backend Engineer  
**Status:** Architecture Audit & Best Practices Guide

---

## 1. System Flow: From Upload to Streaming

Here is a step-by-step breakdown of how the current codebase processes a video file, from the client's initial request to serving the media segments.

```text
[Client]                         [VideoHandler (HTTP)]                [VideoService]                 [FFmpeg]               [Storage Provider]
   │                                       │                                │                           │                       │
   │ 1. POST /api/videos (Multipart File)  │                                │                           │                       │
   │──────────────────────────────────────>│                                │                           │                       │
   │                                       │ 2. Upload(ctx, file, metadata) │                           │                       │
   │                                       │───────────────────────────────>│                           │                       │
   │                                       │                                │ (Saves raw .mp4 to /tmp)  │                       │
   │                                       │                                │                           │                       │
   │                                       │                                │ 3. Spawns Background Task │                       │
   │                                       │                                │    go processAndSaveHLS() │                       │
   │                                       │                                │──┐                        │                       │
   │                                       │                                │  │                        │                       │
   │                                       │                                │<─┘                        │                       │
   │                                       │                                │                           │                       │
   │                                       │                                │ 4. Runs FFmpeg transcode  │                       │
   │                                       │                                │──────────────────────────>│                       │
   │                                       │                                │                           │                       │
   │                                       │                                │ 5. Generates HLS segments │                       │
   │                                       │                                │<──────────────────────────│                       │
   │                                       │                                │                           │                       │
   │                                       │                                │ 6. Saves HLS segments     │                       │
   │                                       │                                │──────────────────────────────────────────────────>│
   │                                       │                                │                           │                       │
   │                                       │                                │ 7. Writes done <- true    │                       │
   │                                       │                                │   (Deferred execution)    │                       │
   │                                       │                                │──┐                        │                       │
   │                                       │                                │  │                        │                       │
   │                                       │                                │<─┘                        │                       │
   │                                       │                                │                           │                       │
   │                                       │                                │ 8. Reads <-done channel   │                       │
   │                                       │                                │    (UNBLOCKS execution)   │                       │
   │                                       │                                │──┐                        │                       │
   │                                       │                                │  │                        │                       │
   │                                       │                                │<─┘                        │                       │
   │                                       │                                │                           │                       │
   │                                       │ 9. Returns *MediaObject        │                           │                       │
   │                                       │<───────────────────────────────│                           │                       │
   │                                       │                                │                           │                       │
   │ 10. HTTP 201 Created (JSON response)  │                                │                           │                       │
   │<──────────────────────────────────────│                                │                           │                       │

======================================== PLAYBACK PHASE ========================================

[Client]                         [VideoHandler (HTTP)]                                                                  [Storage Provider]
   │                                       │                                                                                    │
   │ 11. GET /api/videos/url?key=...       │                                                                                    │
   │──────────────────────────────────────>│                                                                                    │
   │                                       │                                                                                    │
   │ 12. HTTP 200 (Playback URL JSON)      │                                                                                    │
   │<──────────────────────────────────────│                                                                                    │
   │                                       │                                                                                    │
   │ 13. GET /media/.../playlist.m3u8      │                                                                                    │
   │───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────>│
   │                                       │                                                                                    │
   │ 14. Returns HLS Playlist File         │                                                                                    │
   │<───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────│
   │                                       │                                                                                    │
   │ 15. GET /media/.../segment0.ts        │                                                                                    │
   │───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────>│
   │                                       │                                                                                    │
   │ 16. Streams raw MPEG-TS video chunks  │                                                                                    │
   │<───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────│
```

### Flow Breakdown
1. **HTTP Ingestion**: The client issues a multipart POST request with the file. `VideoHandler.Upload` limits parsing to `50 << 20` bytes (50MB) and invokes `VideoService.Upload`.
2. **Raw File Persistence**: The service immediately copies the uploaded stream into a temporary path `/tmp/raw_videos/<uuid>.mp4`.
3. **The Transcode Goroutine**: A background goroutine is spawned to handle the transcoding. It discards the HTTP request context, using `context.Background()`, and starts `processAndSaveHLS`.
4. **Transcoding (FFmpeg)**: FFmpeg is run as a subprocess to transcode the video into HLS formats (creating `playlist.m3u8` and multiple `.ts` video chunks of 10 seconds each) in a temporary directory `/tmp/hls_<uuid>`.
5. **Segment Persistence**: The service reads the HLS directory, loops over the files, and uses the `StorageProvider` to save each segment to the final destination (`./storage/videos/YYYY/MM/<uuid>/...`).
6. **Synchronous Barrier**: Although the transcode was spawned in a goroutine, `VideoService.Upload` blocks on `<-done` right after launching it. It waits for the transcode to finish entirely before returning the HTTP response.
7. **Streaming / Delivery**: The handler returns the playback URL pointing to `/media/<path-to-playlist.m3u8>`. The client plays this stream using HLS, hitting the standard HTTP File Server running on `/media/*`.

---

## 2. Senior Developer Critique: Worst Practices Followed

### 🚨 1. Pseudo-Concurrency & Blocking Channels (The "Done" Anti-Pattern)
You spawned a goroutine in `Upload` but immediately blocked on it using `<-done` on line 125 of [video.go](file:///home/cicada/dev/golang/projects/fileupload/service/video.go#L125).
```go
go func() {
    bgCtx := context.Background()
    v.processAndSaveHLS(bgCtx, videoID, tempSourcePath, finalBasePath, done)
}()
...
<-done // <--- Synchronous Block
```
* **Why it's bad**: This completely defeats the purpose of running the work in a goroutine. The HTTP request handler is blocked waiting for `ffmpeg` to execute. Transcoding takes time (often proportional to the length of the video). Under load, this will hold HTTP threads open, consume connection pools, hit gateway timeout thresholds (usually 30-60s), and cause server exhaustion.
* **Verdict**: If you wanted it synchronous, you shouldn't have used a goroutine. If you wanted it asynchronous, you shouldn't block the request response loop.

---

### 🚨 2. High Risk of Goroutine Leaks
The `done` channel is an unbuffered channel (`make(chan bool)`).
```go
done := make(chan bool)
```
* **Why it's bad**: If any step before `<-done` returns early due to an error, or if we decide to implement a timeout on the HTTP side and return early, the writing side (`done <- true` inside the deferred function on line 43) will block **forever** waiting for a reader that has already left. This leaks the goroutine, keeping its memory and resources pinned indefinitely.
* **Verdict**: Never write to an unbuffered channel in a goroutine without a timeout or a `select` statement unless you are absolutely guaranteed that the receiving goroutine is waiting.

---

### 🚨 3. Severe Error Swallowing and Silent Failures
Errors are logged to standard output but completely ignored in terms of execution flow.
* In [video.go](file:///home/cicada/dev/golang/projects/fileupload/service/video.go#L51):
  ```go
  entries, err := os.ReadDir(tempHLSDir)
  if err != nil {
      return // Silently exits. "done <- true" triggers, and handler thinks upload succeeded!
  }
  ```
* In [video.go](file:///home/cicada/dev/golang/projects/fileupload/service/video.go#L68-L71):
  ```go
  file, err := os.Open(tempFilePath)
  if err != nil {
      continue // Silently skips this segment. The playlist will request it, causing a 404.
  }
  ```
* In [video.go](file:///home/cicada/dev/golang/projects/fileupload/service/video.go#L83-L85):
  ```go
  if err != nil {
      fmt.Printf("failed to transcode %s", finalObjectKey) // Wrong print format, and doesn't handle the error!
  }
  ```
* **Why it's bad**: The user receives a `201 Created` status indicating their video is ready, but in reality, the file copy failed, the segments are incomplete, or the transcode errored out completely.

---

### 🚨 4. In-Process Heavy Compute (Subprocess Resource Exhaustion)
Executing `ffmpeg` inside the same container/server hosting the API (`exec.CommandContext`) is dangerous.
* **Why it's bad**: Video transcoding is highly CPU and memory intensive. If 5 users upload a 1080p video simultaneously, the server will spike CPU to 100%, causing the Go HTTP server to become unresponsive to other API requests, drop connections, or be terminated by the OS Out-Of-Memory (OOM) killer.

---

### 🚨 5. Abandoning the Request Context (`context.Background()`)
Inside `Upload`, the code discards `ctx` (request context) and spawns the goroutine with a fresh `context.Background()`.
```go
bgCtx := context.Background()
v.processAndSaveHLS(bgCtx, ...)
```
* **Why it's bad**: While this was likely done so that client disconnections don't kill the transcode mid-way, it prevents you from passing request traces, transaction IDs, or cancellation signals. It also means you cannot enforce a system-wide transcode timeout (e.g. kill the transcode if it takes more than 10 minutes).

---

### 🚨 6. Disk Accumulation and Orphaned Temp Files
If the application crashes, gets killed mid-transcode, or exits abruptly, files inside `/tmp/raw_videos` and `/tmp/hls_<uuid>` will never be cleaned up because the `defer os.RemoveAll(...)` will not run. Over time, the host's root disk partition will fill up, causing a complete system failure.

---

## 3. Best Practices to Adopt

When building a production-grade video upload and streaming pipeline in Go, we should apply these patterns:

1. **Decouple Ingestion from Processing (Asynchronous Architecture)**:
   * Accept the upload, save the raw file, generate a Job ID, and return `202 Accepted` immediately.
   * Offload the transcode job to an asynchronous worker queue (e.g., using `Redis` and `Asynq`, or a Go worker pool with a bounded channel).
2. **Strict Concurrency Control**:
   * Limit the number of concurrent transcode jobs running on a single machine (e.g., maximum 2 or 3 active transcodes depending on CPU cores).
3. **Structured Context & Error Handling**:
   * Propagate contexts with deadlines/timeouts.
   * Return errors to the caller, wrap errors using `%w`, and clean up resources under all conditions (even panics).
4. **Buffered Channels for Status Delivery**:
   * Use buffered channels of size 1 to prevent goroutine blockage if a sender finishes and no one is listening.
5. **Idempotency and Atomic Saves**:
   * Do not write HLS files one-by-one directly to public storage while transcoding is incomplete. If the transcode fails halfway, you have left a broken folder. Save them in temporary storage first, and only publish them once the manifest (`.m3u8`) is fully generated.

---

## 4. Refactoring Blueprints: Adapting the Best Practices

Let's look at how to rewrite this logic using clean, professional Go patterns.

### Blueprint A: The Bounded Worker Pool (In-Process Async)
If we must run the transcode within the same service, we should use a bounded worker pool. This ensures that no matter how many uploads happen, we only run a fixed number of CPU-heavy `ffmpeg` processes simultaneously.

Here is the design for an asynchronous transcoding queue in the service layer:

```go
package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/arjun118/fileupload/internal/media"
	"github.com/google/uuid"
)

type TranscodeJob struct {
	ID             string
	TempSourcePath string
	FinalBasePath  string
	Ctx            context.Context
}

type VideoService struct {
	storage    media.StorageProvider
	delivery   media.DeliveryProvider
	jobQueue   chan TranscodeJob
	workerWg   sync.WaitGroup
	maxWorkers int
}

func NewVideoService(storage media.StorageProvider, delivery media.DeliveryProvider, maxWorkers int) *VideoService {
	svc := &VideoService{
		storage:    storage,
		delivery:   delivery,
		jobQueue:   make(chan TranscodeJob, 100), // Buffer size for queued jobs
		maxWorkers: maxWorkers,
	}
	svc.startWorkerPool()
	return svc
}

// startWorkerPool launches a fixed number of workers to process transcode jobs
func (v *VideoService) startWorkerPool() {
	for i := 0; i < v.maxWorkers; i++ {
		v.workerWg.Add(1)
		go func(workerID int) {
			defer v.workerWg.Done()
			log.Printf("Starting transcode worker %d", workerID)
			for job := range v.jobQueue {
				log.Printf("Worker %d starting job %s", workerID, job.ID)
				
				// Enforce a hard timeout per transcode job (e.g., 10 minutes)
				jobCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				
				err := v.processAndSaveHLS(jobCtx, job.ID, job.TempSourcePath, job.FinalBasePath)
				if err != nil {
					log.Printf("[Error] Job %s failed: %v", job.ID, err)
					// In production, update database job status to 'failed' here
				} else {
					log.Printf("Job %s completed successfully", job.ID)
					// In production, update database job status to 'completed' here
				}
				cancel()
			}
		}(i)
	}
}

// Stop gracefully shuts down the worker pool
func (v *VideoService) Stop() {
	close(v.jobQueue)
	v.workerWg.Wait()
}
```

### Blueprint B: Robust Asynchronous Transcoding Flow
This function is redesigned to properly handle errors, wrap them, and clean up temp files under all failure scenarios.

```go
func (v *VideoService) processAndSaveHLS(ctx context.Context, videoID string, tempSourcePath string, finalBasePath string) (err error) {
	tempHLSDir := filepath.Join("/tmp", "hls_"+videoID)
	
	if err := os.MkdirAll(tempHLSDir, 0755); err != nil {
		return fmt.Errorf("failed to create HLS temp dir: %w", err)
	}

	// Double-defer pattern to clean up files and recover panics
	defer func() {
		os.RemoveAll(tempHLSDir)
		os.Remove(tempSourcePath)
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered during transcoding: %v", r)
		}
	}()

	// 1. Run the Transcode
	_, err = TranscodeToHLS(ctx, tempSourcePath, tempHLSDir)
	if err != nil {
		return fmt.Errorf("transcode failed: %w", err)
	}

	// 2. Read HLS Directory
	entries, err := os.ReadDir(tempHLSDir)
	if err != nil {
		return fmt.Errorf("failed to read output directory: %w", err)
	}

	// 3. Save files to permanent storage
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		
		fileName := entry.Name()
		tempFilePath := filepath.Join(tempHLSDir, fileName)
		finalObjectKey := filepath.ToSlash(filepath.Join(finalBasePath, fileName))

		if err := v.saveSegment(ctx, tempFilePath, finalObjectKey, fileName); err != nil {
			return fmt.Errorf("failed to save segment %s: %w", fileName, err)
		}
	}

	return nil
}

// Helper to save a single segment and handle file descriptors properly
func (v *VideoService) saveSegment(ctx context.Context, srcPath, destKey, fileName string) error {
	file, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer file.Close()

	contentType := "video/mp2t"
	if filepath.Ext(fileName) == ".m3u8" {
		contentType = "application/vnd.apple.mpegurl"
	}

	meta := media.FileMetaData{
		Filename:    fileName,
		ContentType: contentType,
	}

	_, err = v.storage.Save(ctx, destKey, file, meta)
	if err != nil {
		return fmt.Errorf("storage save failed: %w", err)
	}
	return nil
}
```

### Blueprint C: Ingestion Handler Refactored to `202 Accepted`
Instead of blocking on channel notification `<-done`, we write the raw file, submit the task to the queue, and return immediately.

```go
func (v *VideoService) Upload(ctx context.Context, file io.Reader, meta media.FileMetaData) (*media.MediaObject, error) {
	videoID := uuid.NewString()
	
	// Create a temp folder for raw files
	tempRawDir := filepath.Join("/tmp", "raw_videos")
	if err := os.MkdirAll(tempRawDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	
	tempSourcePath := filepath.Join(tempRawDir, videoID+".mp4")
	tempFile, err := os.Create(tempSourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	
	// Copy file data to temporary path
	size, err := io.Copy(tempFile, file)
	tempFile.Close() // Close file stream immediately
	if err != nil {
		os.Remove(tempSourcePath)
		return nil, fmt.Errorf("failed to save raw file upload: %w", err)
	}

	finalBasePath := v.generateHLSFolderPath(videoID)
	playlistKey := filepath.ToSlash(filepath.Join(finalBasePath, "playlist.m3u8"))
	
	playBackURL, err := v.delivery.URL(ctx, playlistKey)
	if err != nil {
		os.Remove(tempSourcePath)
		return nil, fmt.Errorf("failed to generate playback URL: %w", err)
	}

	// -------------------------------------------------------------
	// Dispatch Job to Worker Queue (Non-blocking)
	// -------------------------------------------------------------
	job := TranscodeJob{
		ID:             videoID,
		TempSourcePath: tempSourcePath,
		FinalBasePath:  finalBasePath,
		Ctx:            context.Background(), // Decoupled from HTTP lifecycle
	}
	
	select {
	case v.jobQueue <- job:
		log.Printf("Job %s successfully queued", videoID)
	default:
		// Queue is full! Handle backpressure.
		os.Remove(tempSourcePath)
		return nil, fmt.Errorf("server queue is full, please try again later")
	}

	// Return immediate metadata representation with 202 Accepted status
	return &media.MediaObject{
		ID:          videoID,
		ObjectKey:   playlistKey,
		Provider:    "local",
		ContentType: "application/vnd.apple.mpegurl",
		Size:        size,
		PlaybackURL: playBackURL,
	}, nil
}
```

---

## 5. Architectural Recommendations for Scale

If you plan to scale this system beyond a simple prototype, consider these architectural changes:

1. **Off-load Transcoding to Serverless / SaaS**:
   * Save raw uploads directly to an S3/MinIO bucket.
   * Hook an S3 event trigger (Lambda or cloud function) to spawn an **FFmpeg task** or call **AWS Elemental MediaConvert**.
   * This completely frees your API servers from CPU-intensive video encoding.
2. **Implement DB-Driven Job Status**:
   * Add a table for video statuses: `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`.
   * Return the video ID and status to the client. The client can poll `/api/videos/<id>/status` or open a WebSocket to update the UI (e.g. progress bar).
3. **Use a Robust Distributed Task Queue**:
   * Switch the in-memory `jobQueue` to a persistent queue engine like **Temporal**, **Asynq (Redis)**, or **RabbitMQ**. This guarantees that if your application restarts, you do not lose processing jobs.

---

## 6. Graceful Shutdown & Stopping the Worker Pool

### Where and How to Call `StopWorkerPool`
The `StopWorkerPool` function must be called in `main.go` when the application receives an OS termination signal (`SIGINT`, `SIGTERM`). 

#### Critical Shutdown Order
1. **Shut down the HTTP Server first**: This stops new connections. If you shut down the worker pool first, incoming uploads will try to queue tasks onto a closed channel, triggering a Go runtime panic (`panic: send on closed channel`).
2. **Stop the Worker Pool second**: Once the HTTP server is closed, calling `StopWorkerPool()` will close the channel and wait for the active worker goroutines to complete their current transcoding tasks.

#### Graceful Shutdown Pattern for `main.go`
```go
// in main.go
func main() {
    // ... initialize services ...
    videoService := service.NewVideoService(storageProvider, deliverProvider, 3)
    
    server := &http.Server{
        Addr:    ":8080",
        Handler: router,
    }

    // Run HTTP server in background
    go func() {
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("Server failed: %v", err)
        }
    }()

    // Listen for shutdown signals
    stop := make(chan os.Signal, 1)
    signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
    <-stop // Block until signal is received

    log.Println("Shutting down gracefully...")

    // 1. Close HTTP server (give active requests 15 seconds to wrap up)
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    server.Shutdown(ctx)

    // 2. Shut down background workers (waits for active transcodes to finish)
    videoService.StopWorkerPool()
    
    log.Println("Clean exit completed.")
}
```

---

## 7. Best Practices: Error Handling in Background Workers

In Go, when you run background tasks in goroutines, standard HTTP error handling (`http.Error`) is impossible because the client connection is already closed. You must follow these patterns to prevent panics, monitor health, and handle failures cleanly:

### 1. Panic Recovery is Mandatory
A panic in a goroutine that is not recovered will **crash your entire Go process**. Every background worker loop must defer a recovery block.
```go
go func() {
    defer func() {
        if r := recover(); r != nil {
            log.Printf("[CRITICAL PANIC] Worker crashed: %v\nStack trace: %s", r, debug.Stack())
            // Send alert to Sentry/PagerDuty here
        }
    }()
    // Worker logic here...
}()
```

### 2. Double-Defer Cleanup
Background processes often allocate temporary files or OS descriptors. Ensure cleanups execute regardless of whether the function returns an error or panics:
```go
func (v *VideoService) processAndSaveHLS(...) (err error) {
    // 1. Define cleanups inside defer
    defer func() {
        os.RemoveAll(tempHLSDir)
        os.Remove(tempSourcePath)
        // 2. Capture panics and translate them to Go errors
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in transcoder: %v", r)
        }
    }()
    
    // 3. Do actual work
    err = TranscodeToHLS(...)
    return err
}
```

### 3. Monitoring & Alerting
When a background job fails, write logs using structured logging (`log/slog` or `logrus`) and emit metric counters to telemetry (e.g., Prometheus `transcode_failures_total`).
```go
if err != nil {
    slog.Error("job failed", 
        "job_id", job.ID, 
        "error", err,
        "duration_sec", time.Since(start).Seconds(),
    )
    metrics.Increment("transcode_failures")
}
```

---

## 8. Production Architecture: Division of Responsibilities

In a production environment, **the Go API server should never run FFmpeg subprocesses**. Heavy compute jobs must be decoupled from the request-response cycle.

### Who Handles What?

| Component | Responsibility | Environment / Tech |
| :--- | :--- | :--- |
| **Go API Server** | Handles client requests, authenticates users, generates metadata records in the DB, and orchestrates upload destinations. | Lightweight containers (ECS/K8s/App Engine) |
| **Storage (Object)** | Ingests raw videos directly from clients (via pre-signed URLs) and hosts transcoded HLS streams. | Amazon S3 / Google Cloud Storage / MinIO |
| **Task Queue** | Manages queueing, retries, backpressure, and worker job routing. | Redis (Asynq/Celery), RabbitMQ, Amazon SQS, Temporal |
| **Transcoding Worker** | Subscribes to the queue, downloads raw video from Storage, transcodes it, uploads HLS segments back to Storage, and notifies the API. | GPU-optimized VM scale-set or Serverless (AWS MediaConvert) |

### The Production Pipeline Flow

```text
[Client]                [Go API Server]              [Database]             [Task Queue]            [Transcoding Worker]
   │                           │                         │                       │                        │
   │ 1. POST /api/videos       │                         │                       │                        │
   │──────────────────────────>│                         │                       │                        │
   │                           │ 2. Create record        │                       │                        │
   │                           │    Status = "pending"   │                       │                        │
   │                           │────────────────────────>│                       │                        │
   │                           │                         │                       │                        │
   │                           │ 3. Push job to queue    │                       │                        │
   │                           │    (VideoID, FilePath)  │                       │                        │
   │                           │────────────────────────────────────────────────>│                        │
   │                           │                                                 │                        │
   │ 4. Return 202 Accepted    │                                                 │                        │
   │    (VideoID, Job Queued)  │                                                 │                        │
   │<──────────────────────────│                                                 │                        │
   │                           │                                                 │ 5. Pulls Transcode Job │
   │                           │                                                 │<───────────────────────│
   │                           │                                                 │                        │
   │                           │                                                 │ 6. Executes Transcode  │
   │                           │                                                 │    (SaaS / MediaConvert│
   │                           │                                                 │     or GPU Server)     │
   │                           │                                                 │───────────────────────┐│
   │                           │                                                 │                       ││
   │                           │                                                 │<──────────────────────┘│
   │                           │                                                 │                        │
   │                           │                                                 │ 7. Uploads HLS segments│
   │                           │                                                 │    to S3/CDN Bucket    │
   │                           │                                                 │───────────────────────┐│
   │                           │                                                 │                       ││
   │                           │                                                 │<──────────────────────┘│
   │                           │                                                 │                        │
   │                           │ 8. Update DB (Status = "completed")             │                        │
   │                           │<─────────────────────────────────────────────────────────────────────────│
```

### Job Error Flow in Production
If the transcoding worker fails:
1. **The API server is unaffected**: Client playback and other operations remain 100% responsive.
2. **Database Status updated**: The worker updates the database record for the video ID to `failed` and records the error log stack.
3. **Queue Reprocessing/Retries**: If the error was transient (e.g. network timeout downloading raw video), the queue engine automatically retries the task up to $N$ times with exponential backoff.
4. **Dead Letter Queue (DLQ)**: If the transcode fails permanently (e.g., corrupt video file), the job is routed to a Dead Letter Queue for developer investigation, and the user receives a "failed to process video" notification.

---

## 9. Better to Implement Now

Before integrating the POC with your actual application, you should address the following elements to ensure the pipeline is robust and matches production requirements.

### 1. MinIO Storage Provider & HLS Delivery
You currently have an empty [minio/provider.go](file:///home/cicada/dev/golang/projects/fileupload/internal/media/minio/provider.go). You need to implement the [StorageProvider](file:///home/cicada/dev/golang/projects/fileupload/internal/media/provider.go#L25) interface for MinIO.

Additionally, test how HLS player clients will retrieve HLS streams:
*   **Public Bucket**: Bucket policy allows read-only public access. Playback URLs point directly to MinIO: `http://<minio-host>/<bucket>/videos/.../playlist.m3u8`.
*   **Proxy Route**: Keep the bucket private. Implement a reverse proxy route in your Go router (`r.Handle("/media/*", proxyToMinIO)`) that streams segments to the player by fetching them from MinIO behind the scenes.

#### HLS Delivery: Detailed Implementation Options

##### Option A: Public Bucket Access
In this model, the client's video player requests HLS files directly from MinIO, bypassing the Go backend for playback.

1. **How to Implement**:
   * **Configure Bucket Policy**: Set the bucket access policy to public read-only (either manually in the MinIO UI or programmatically via `client.SetBucketPolicy`).
   * **Playback URL Generation**: The [DeliveryProvider](file:///home/cicada/dev/golang/projects/fileupload/internal/media/provider.go#L30) implementation simply formats the public address:
     ```go
     func (d *MinioDelivery) URL(ctx context.Context, objectKey string) (string, error) {
         // e.g., http://localhost:9000/videos/videos/2026/06/video_id/playlist.m3u8
         return fmt.Sprintf("http://%s/%s/%s", d.Endpoint, d.Bucket, objectKey), nil
     }
     ```
2. **Trade-offs**:
   * **Pros**: Ultra-low latency, zero CPU/memory overhead on your API servers since streaming traffic goes directly to MinIO or a CDN.
   * **Cons**: No access control. Anyone with the URL can view/share the video.

##### Option B: Go API Reverse Proxy Route (Private Bucket)
In this model, the MinIO bucket remains completely private. The video player requests segments from your Go backend (e.g., `http://localhost:8080/media/...`), which programmatically retrieves them from MinIO and streams them to the client.

1. **How to Implement**:
   * **Private Bucket Policy**: Keep the bucket private (the default).
   * **Go Router Handler**: Create a route that intercepts requests prefix-matched with `/media/*`, checks authentication (cookies, sessions, JWTs), fetches the requested object from MinIO, and copies the stream to the HTTP response.
   * **Proxy Handler Code**:
     ```go
     func NewMinioProxyHandler(minioClient *minio.Client, bucketName string) http.HandlerFunc {
         return func(w http.ResponseWriter, r *http.Request) {
             // 1. Authentication check
             // if !isAuthorized(r) { http.Error(w, "Unauthorized", http.StatusUnauthorized); return }

             // 2. Extract path (e.g. "videos/2026/06/id/playlist.m3u8")
             objectKey := strings.TrimPrefix(r.URL.Path, "/media/")
             if objectKey == "" {
                 http.Error(w, "invalid path", http.StatusBadRequest)
                 return
             }

             // 3. Get object reader from MinIO
             obj, err := minioClient.GetObject(r.Context(), bucketName, objectKey, minio.GetObjectOptions{})
             if err != nil {
                 http.Error(w, "failed to get media object", http.StatusNotFound)
                 return
             }
             defer obj.Close()

             // Check for existence and read metadata
             stat, err := obj.Stat()
             if err != nil {
                 http.Error(w, "media not found", http.StatusNotFound)
                 return
             }

             // 4. Set headers and stream the payload
             w.Header().Set("Content-Type", stat.ContentType)
             w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size))
             
             // Copy the stream chunk-by-chunk to the client
             _, _ = io.Copy(w, obj)
         }
     }
     ```
   * **Register the Route in `main.go`**:
     ```go
     r.Get("/media/*", NewMinioProxyHandler(minioClient, "videos"))
     ```
2. **Trade-offs**:
   * **Pros**: Full security control. You can restrict video playback to authenticated owners or authorized groups, and your storage endpoints are hidden from the public internet.
   * **Cons**: Significant API server resource consumption. Every chunk of streaming video is proxied through Go, consuming API network bandwidth, memory buffers, and connection pools.


#### MinIO Storage Blueprint (`internal/media/minio/provider.go`)
```go
package minio

import (
	"context"
	"fmt"
	"io"

	"github.com/arjun118/fileupload/internal/media"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Storage struct {
	client     *minio.Client
	bucketName string
}

func NewStorage(endpoint, accessKey, secretKey, bucketName string, useSSL bool) (*Storage, error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}

	return &Storage{
		client:     minioClient,
		bucketName: bucketName,
	}, nil
}

func (s *Storage) Save(ctx context.Context, objectKey string, r io.Reader, meta media.FileMetaData) (int64, error) {
	info, err := s.client.PutObject(ctx, s.bucketName, objectKey, r, meta.Size, minio.PutObjectOptions{
		ContentType: meta.ContentType,
	})
	if err != nil {
		return 0, fmt.Errorf("minio upload failed: %w", err)
	}
	return info.Size, nil
}

func (s *Storage) Delete(ctx context.Context, objectKey string) error {
	err := s.client.RemoveObject(ctx, s.bucketName, objectKey, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("minio delete failed: %w", err)
	}
	return nil
}
```

### 2. Database Integration & Video Job States
Because transcoding is an asynchronous background process, returning a `201 Created` with a playback URL immediately is misleading if transcoding fails later. The frontend needs to track progress:
*   **Database Table / Model**: Store video metadata along with job states: `QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`.
*   **State Updates**: Inside your worker loop in [video.go](file:///home/cicada/dev/golang/projects/fileupload/service/video.go#L45), update the status in the DB at the start and end of processing (both on success, failure, and panic recovery).
*   **Status Query Endpoint**: Create a `GET /api/videos/{id}` endpoint to let the frontend poll for the current transcoding status.

### 3. Startup Cleanup of Orphaned Temp Files
If the application crashes mid-transcode, temporary files in `/tmp/raw_videos` and `/tmp/hls_*` will accumulate.
*   Implement a startup cleanup function in `main.go` that purges these directories on boot.

