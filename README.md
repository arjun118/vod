
# IMPROVEMENTS (CHANGES TO BE MADE)

# Video Processing Architecture (MinIO/S3 Version)

## Goal

Move away from storing raw uploads on the application server.

Object storage becomes the source of truth.

Workers become disposable and can process any video by downloading it from storage.

---

# Upload Flow

```text
Client
   ↓
Upload Raw Video
   ↓
Object Storage (MinIO/S3)
   ↓
Create Video Record
   ↓
Queue Transcode Job
   ↓
Return Video ID
```

---

# Storage Layout

```text
videos-bucket/

├── raw/
│   └── {video_id}.mp4
│
├── hls/
│   └── {video_id}/
│       ├── playlist.m3u8
│       ├── segment000.ts
│       ├── segment001.ts
│       └── ...
│
└── thumbnails/
    └── {video_id}.jpg
```

Example:

```text
raw/6b0f4a.mp4

hls/6b0f4a/
    playlist.m3u8
    segment000.ts
    segment001.ts

thumbnails/6b0f4a.jpg
```

---

# Upload Service Responsibilities

## Upload Endpoint

```text
POST /videos
```

Responsibilities:

1. Accept upload
2. Save raw file to MinIO/S3
3. Create database record
4. Queue transcode job
5. Return video metadata

The upload endpoint DOES NOT:

* run FFmpeg
* generate HLS
* generate thumbnails

Those are background jobs.

---

# Database Model

```sql
videos
```

Columns:

```text
id
owner_id

status
    uploaded
    processing
    ready
    failed

raw_object_key

playlist_object_key

thumbnail_object_key

created_at
updated_at
```

Example:

```text
status = uploaded

raw_object_key =
raw/6b0f4a.mp4
```

---

# Queue Payload

```json
{
  "video_id": "6b0f4a",
  "raw_key": "raw/6b0f4a.mp4"
}
```

Only minimal information should be sent.

Workers can fetch additional metadata from the database if needed.

---

# Worker Flow

```text
Queue
   ↓
Worker receives job
   ↓
Download raw video
   ↓
Temporary working directory
   ↓
Run FFmpeg
   ↓
Generate HLS files
   ↓
Upload HLS artifacts
   ↓
Generate thumbnail
   ↓
Upload thumbnail
   ↓
Update DB status
```

---

# Worker Processing Flow

## Step 1

Download source video:

```text
raw/{video_id}.mp4
```

to:

```text
/tmp/transcode-{video_id}/input.mp4
```

---

## Step 2

Run FFmpeg:

```text
input.mp4
      ↓
playlist.m3u8
segment000.ts
segment001.ts
...
```

Output directory:

```text
/tmp/transcode-{video_id}/output/
```

---

## Step 3

Upload generated artifacts:

```text
playlist.m3u8
segment000.ts
segment001.ts
...
```

to:

```text
hls/{video_id}/
```

---

## Step 4

Generate thumbnail:

```text
thumbnail.jpg
```

Upload:

```text
thumbnails/{video_id}.jpg
```

---

## Step 5

Update database:

```text
status = ready

playlist_object_key =
hls/{video_id}/playlist.m3u8

thumbnail_object_key =
thumbnails/{video_id}.jpg
```

---

# Temporary Files

Workers are allowed to use local disk.

Example:

```text
/tmp/transcode-{video_id}/
```

Contents:

```text
input.mp4

output/
    playlist.m3u8
    segment000.ts
    ...
```

Cleanup after processing:

```text
Delete temporary directory
```

The temporary directory is NOT a source of truth.

Object storage remains the source of truth.

---

# Delivery Architecture

## Development

```text
Player
   ↓
Go Proxy
   ↓
MinIO
```

Purpose:

* Learn streaming
* Learn HLS
* Learn proxying
* Learn auth

---

## Production

```text
Player
   ↓
Nginx
   ↓
MinIO/S3
```

or

```text
Player
   ↓
CloudFront
   ↓
S3
```

Go should not relay video bytes in production.

---

# Authorization

Application server remains responsible for:

```text
Authentication
Authorization
Ownership checks
Course access checks
```

Storage layer remains responsible for:

```text
Serving media bytes
```

---

# Future Optimizations

## Parallel Segment Uploads

Current:

```text
segment1
segment2
segment3
```

uploaded sequentially.

Future:

```text
8 upload workers

segment1 -> worker1
segment2 -> worker2
segment3 -> worker3
```

---

## Multiple Renditions

Generate:

```text
1080p
720p
480p
```

in parallel jobs.

---

## Signed URLs

Replace proxy architecture with:

```text
Player
   ↓
API
   ↓
Signed URL
   ↓
MinIO/S3
```

---

# Final Architecture

```text
Client
   ↓
Upload
   ↓
MinIO/S3 (raw/)
   ↓
Database Record
   ↓
Queue Job
   ↓
Worker
   ↓
Download raw video
   ↓
FFmpeg
   ↓
Generate HLS
   ↓
Upload hls/
   ↓
Upload thumbnail
   ↓
Update DB
   ↓
Ready

Playback:

Player
   ↓
Nginx / CDN
   ↓
MinIO/S3
```
