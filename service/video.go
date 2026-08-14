package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/arjun118/fileupload/internal/config"
	"github.com/arjun118/fileupload/internal/media"
	"github.com/arjun118/fileupload/internal/outbox"
	"github.com/arjun118/fileupload/internal/queue"
	"github.com/arjun118/fileupload/internal/transcode"
	"github.com/arjun118/fileupload/internal/transcoder"
	"github.com/arjun118/fileupload/internal/video"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

type VideoService struct {
	Transcoder    *transcoder.Transcoder
	Storage       media.StorageProvider
	Delivery      media.DeliveryProvider
	Queue         queue.Queue
	Logger        zerolog.Logger
	Layout        *media.StorageLayout
	DB            *pgxpool.Pool
	VideoRepo     *video.Repository
	TranscodeRepo *transcode.Repository
	OutboxRepo    *outbox.Repository
	Provider      string
}

type transcodeOutput struct {
	MasterPlaylist string
	DashManifest   string
	MediaPlaylists []string
	Segments       []string
}

func classifyOutputFiles(dir string) (*transcodeOutput, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read output directory %w", err)
	}
	out := &transcodeOutput{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case name == "master.m3u8":
			out.MasterPlaylist = name
		case name == "manifest.mpd":
			out.DashManifest = name
		case filepath.Ext(name) == ".m3u8":
			out.MediaPlaylists = append(out.MediaPlaylists, name)
		case filepath.Ext(name) == ".m4s":
			out.Segments = append(out.Segments, name)
		}
	}
	return out, nil
}

func (v *VideoService) uploadStreamAssets(ctx context.Context, localDir string, keys media.VideoKeys, out *transcodeOutput) error {
	// upload segments concurrently
	eg, groupCtx := errgroup.WithContext(ctx)
	eg.SetLimit(10)

	for _, name := range out.Segments {
		fileName := name
		eg.Go(func() error {
			return v.uploadFile(groupCtx, localDir, keys.StreamObjectKey(fileName), fileName)
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("uplaod segments failed: %w", err)
	}
	if out.DashManifest != "" {
		if err := v.uploadFile(ctx, localDir, keys.StreamObjectKey(out.DashManifest), out.DashManifest); err != nil {
			return fmt.Errorf("upload dash manifest: %w", err)
		}
	}
	// Phase 3: Upload rendition playlists
	for _, name := range out.MediaPlaylists {
		if err := v.uploadFile(ctx, localDir, keys.StreamObjectKey(name), name); err != nil {
			return fmt.Errorf("upload rendition playlist %s: %w", name, err)
		}
	}

	// Phase 4: Upload master playlist LAST
	if out.MasterPlaylist != "" {
		if err := v.uploadFile(ctx, localDir, keys.StreamObjectKey(out.MasterPlaylist), out.MasterPlaylist); err != nil {
			return fmt.Errorf("upload master playlist: %w", err)
		}
	}

	return nil

}

func (v *VideoService) WorkDir(videoID string) string {
	return filepath.Join("/tmp/jobs", videoID)
}

func NewVideoService(storage media.StorageProvider, delivery media.DeliveryProvider, db *pgxpool.Pool, q queue.Queue, logger zerolog.Logger, l *media.StorageLayout, videoRepo *video.Repository, transcodeRepo *transcode.Repository, outboxRepo *outbox.Repository, provider string) *VideoService {
	svc := &VideoService{
		Transcoder:    transcoder.New(),
		Storage:       storage,
		Delivery:      delivery,
		Queue:         q,
		Logger:        logger,
		Provider:      provider,
		DB:            db,
		VideoRepo:     videoRepo,
		TranscodeRepo: transcodeRepo,
		OutboxRepo:    outboxRepo,
		Layout:        l,
	}
	return svc
}

func (v *VideoService) ProcessAndSaveHLS(ctx context.Context, videoID string, keys media.VideoKeys) (err error) {

	jobLogger := v.Logger.With().Str("video_id", videoID).Logger()
	workDir := v.WorkDir(videoID)
	outputDir := filepath.Join(workDir, "output")
	storageSourceKey := keys.RawObjectKey

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp local stream dir: %w", err)
	}
	defer func() {
		os.RemoveAll(workDir)
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during transcoding: %v", r)
		}
	}()
	downloadStart := time.Now()
	localSourcePath, err := v.Storage.Get(ctx, storageSourceKey, workDir)
	if err != nil {
		return fmt.Errorf("failed to download source to local: %w", err)
	}
	jobLogger.Info().Dur("duration", time.Since(downloadStart)).Msg("downloaded raw video")

	transcodeStart := time.Now()
	err = v.Transcoder.Transcode(ctx, localSourcePath, outputDir)
	if err != nil {
		return fmt.Errorf("transcode failed: %w", err)
	}
	jobLogger.Info().Dur("duration", time.Since(transcodeStart)).Msg("transcoding complete")

	output, err := classifyOutputFiles(outputDir)
	if err != nil {
		return fmt.Errorf("classfiy output : %w", err)
	}

	uploadStart := time.Now()
	if err := v.uploadStreamAssets(ctx, outputDir, keys, output); err != nil {
		return fmt.Errorf("upload stream assets: %w", err)
	}
	jobLogger.Info().Dur("duration", time.Since(uploadStart)).Msg("stream upload complete")
	return nil
}

func (v *VideoService) uploadFile(ctx context.Context, localDir, objectKey, fileName string) error {
	file, err := os.Open(filepath.Join(localDir, fileName))
	if err != nil {
		return fmt.Errorf("open %s: %w", fileName, err)
	}
	defer file.Close()
	// uploadFile will use saveStream
	_, err = v.Storage.SaveStream(ctx, objectKey, file)
	if err != nil {
		return fmt.Errorf("storage save of segment failed: %w", err)
	}
	return nil
}

func (v *VideoService) Upload(ctx context.Context, file io.Reader, meta media.FileMetaData) (*media.MediaObject, error) {
	cfg := config.Load()
	videoID := uuid.NewString()

	uploadLogger := v.Logger.With().Str("video_id", videoID).Logger()
	uploadLogger.Info().Msg("uploading the raw file to minio raw bucket...")

	// rawUploadContext, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	// defer cancel()
	keys := v.Layout.ForVideo(videoID)
	// videos/year/month/videoid/raw.mp4
	rawObjectKey := keys.RawObjectKey
	// videos/2026/06/video_uuid/master.m3u8
	playlistKey := keys.PlaylistKey
	uploadStart := time.Now()
	size, err := v.Storage.SaveRaw(ctx, rawObjectKey, file)
	if err != nil {
		return nil, fmt.Errorf("raw video storage save failed: %w", err)
	}

	uploadLogger.Info().Float64("size_mb", float64(size)/float64(1024*1024)).Dur("duration", time.Since(uploadStart)).Msg("raw file uploaded successfully")

	playBackURL, err := v.Delivery.URL(ctx, playlistKey)
	if err != nil {
		// os.Remove(tempSourcePath)
		return nil, fmt.Errorf("failed to generate playback URL: %w", err)
	}
	// write to
	// begin tx
	// 1. videos table
	// 2. transcode jobs table
	// 3. outbox table
	// commit
	tx, err := v.DB.Begin(ctx)
	if err != nil {
		// v.Logger.Error().Err(err).Msg("failed to initiate db transaction")
		return nil, fmt.Errorf("failed to initiate db transaction : %w", err)
	}
	var video video.Video
	video.SizeBytes = size
	video.StorageKey = keys.RawObjectKey
	video.PlaylistKey = keys.PlaylistKey
	video.PlaybackURL = playBackURL
	video.UploadStatus = "success"
	// store video
	err = v.VideoRepo.Create(ctx, tx, &video)
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("failed to store video data: %w", err)
	}

	// store transode job
	var jobModel transcode.Job
	jobModel.MaxAttempts = cfg.MaxTranscodeAttempts
	jobModel.VideoID = video.ID
	err = v.TranscodeRepo.Create(ctx, tx, &jobModel)
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("failed to store transcode job: %w", err)
	}
	// store outbox record
	transcodeJob := transcode.TranscodeJob{
		VideoID:          video.ID.String(),
		StorageSourceKey: video.StorageKey,
		PlaylistKey:      video.PlaylistKey,
	}
	payload, err := json.Marshal(transcodeJob)
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("failed to create outbox event : %w", err)
	}
	var event outbox.Event
	event.EventType = outbox.EventTranscodeRequest
	event.AggregateID = jobModel.ID
	event.Payload = payload
	err = v.OutboxRepo.Create(ctx, tx, &event)
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("failed to store outbox record: %w", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to write video metadata and job details to db: %w", err)
	}
	return &media.MediaObject{
		ID:          videoID,
		ObjectKey:   playlistKey,
		Provider:    v.Provider,
		ContentType: "application/vnd.apple.mpegurl",
		Size:        size,
		PlaybackURL: playBackURL,
	}, nil
}

func (s *VideoService) GetPlaybackURL(ctx context.Context, objectKey string) (string, error) {
	url, err := s.Delivery.URL(ctx, objectKey)
	if err != nil {
		return "", fmt.Errorf("failed to get playback URL for key %s: %w", objectKey, err)
	}

	return url, nil
}

// Delete removes a video from the storage provider (and eventually the database).
func (s *VideoService) Delete(ctx context.Context, objectKey string) error {
	err := s.Storage.DeleteRaw(ctx, objectKey)
	if err != nil {
		return fmt.Errorf("failed to delete video from storage: %w", err)
	}
	return nil
}
