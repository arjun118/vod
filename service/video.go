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
	"github.com/arjun118/fileupload/internal/transcoder"
	"github.com/google/uuid"
)

type TranscodeJob struct {
	VideoID string
	//this will be the object key for the source video
	// need to download from this
	StorageSourceKey string
}

type VideoService struct {
	transcoder *transcoder.Transcoder
	storage    media.StorageProvider
	delivery   media.DeliveryProvider
	jobQueue   chan TranscodeJob
	workerWg   sync.WaitGroup
	maxWorkers int
	Provider   string
}

func (v *VideoService) generateStorageSourceKey(videoID string) string {
	now := time.Now()
	year := now.Format("2006")
	month := now.Format("01")
	return filepath.ToSlash(filepath.Join("videos", year, month, videoID, "raw.mp4"))
}

func (v *VideoService) generateStorageStreamKey(videoID string) string {
	//this is also playlist key
	now := time.Now()
	year := now.Format("2006")
	month := now.Format("01")
	return filepath.ToSlash(filepath.Join("videos", year, month, videoID, "master.m3u8"))
}

func (v *VideoService) generateStorageStreamFolder(videoID string) string {
	now := time.Now()
	year := now.Format("2006")
	month := now.Format("01")
	return filepath.ToSlash(filepath.Join("videos", year, month, videoID))
}

func (v *VideoService) workDir(videoID string) string {
	return filepath.Join("/tmp/jobs", videoID)
}

func NewVideoService(storage media.StorageProvider, delivery media.DeliveryProvider, maxWorkers int, provider string) *VideoService {
	svc := &VideoService{
		transcoder: transcoder.New(),
		storage:    storage,
		delivery:   delivery,
		//buffer size for queued jobs
		jobQueue:   make(chan TranscodeJob, 100),
		maxWorkers: maxWorkers,
		Provider:   provider,
	}
	svc.StartWorkerPool()
	return svc
}

func (v *VideoService) StartWorkerPool() {
	for i := range v.maxWorkers {
		v.workerWg.Add(1)
		go func(workerID int) {
			defer v.workerWg.Done()
			log.Printf("starting transcode worker %d", workerID)
			for job := range v.jobQueue {
				log.Printf("Worker %d starting job %s", workerID, job.VideoID)
				// /tmp/jobs/video_id
				workDir := v.workDir(job.VideoID)
				output := filepath.Join(workDir, "output")
				jobCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				err := v.processAndSaveHLS(jobCtx, job.VideoID, job.StorageSourceKey, output)
				if err != nil {
					log.Printf("[Error] Job %s failed: %v", job.VideoID, err)
					//prod: update database job status - failed
				} else {
					log.Printf("Job %s completed successfully", job.VideoID)
				}
				cancel()
			}
		}(i)
	}
}

func (v *VideoService) StopWorkerPool() {
	close(v.jobQueue)
	v.workerWg.Wait()
}

func (v *VideoService) processAndSaveHLS(ctx context.Context, videoID string, storageSourceKey string, localStreamFolder string) (err error) {
	if err := os.MkdirAll(localStreamFolder, 0755); err != nil {
		return fmt.Errorf("failed to create temp local stream dir: %w", err)
	}
	localSourceFolder := v.workDir(videoID)
	localSourcePath, err := v.storage.Get(ctx, storageSourceKey, localSourceFolder)
	if err != nil {
		return fmt.Errorf("failed to download source to local: %w", err)
	}
	//clean the temporary folders
	defer func() {
		os.RemoveAll(localStreamFolder)
		os.Remove(localSourcePath)
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered during transcoding: %v", r)
		}
	}()
	//localSourcePath - /tmp/jobs/vid_id/raw.mp4
	// localStreamFolder  - /tmp/jobs/vid_id/output
	err = v.transcoder.Transcode(ctx, localSourcePath, localStreamFolder)
	if err != nil {
		return fmt.Errorf("transcode failed: %w", err)
	}
	entries, err := os.ReadDir(localStreamFolder)
	if err != nil {
		return fmt.Errorf("failed to read output directory %w", err)
	}
	for _, entry := range entries {

		if entry.IsDir() {
			continue
		}
		//master.m3u8 or manifest.mpd or any chunk for hls/dash
		fileName := entry.Name()
		localSegmentPath := filepath.Join(localStreamFolder, fileName)
		storageStreamFolder := v.generateStorageStreamFolder(videoID)
		finalObjectKey := filepath.ToSlash(filepath.Join(storageStreamFolder, fileName))
		if err := v.saveSegment(ctx, localSegmentPath, finalObjectKey, fileName); err != nil {
			return fmt.Errorf("failed to save segment %s: %w", fileName, err)
		}
	}
	return nil
}

func (v *VideoService) saveSegment(ctx context.Context, srcPath, destKey, fileName string) error {
	file, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer file.Close()
	contentType := media.ContentTypeFromExtension(fileName)
	meta := media.FileMetaData{
		Filename:    fileName,
		ContentType: contentType,
	}
	// saveSegment will use saveStream
	_, err = v.storage.SaveStream(ctx, destKey, file, meta)
	if err != nil {
		return fmt.Errorf("storage save of segment failed: %w", err)
	}
	return nil
}

func (v *VideoService) Upload(ctx context.Context, file io.Reader, meta media.FileMetaData) (*media.MediaObject, error) {
	videoID := uuid.NewString()
	log.Println("uploading the raw file to minio raw bucket...")
	rawUploadContext, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// videos/year/month/videoid/raw.mp4
	rawObjectKey := v.generateStorageSourceKey(videoID)
	size, err := v.storage.SaveRaw(rawUploadContext, rawObjectKey, file, media.FileMetaData{})
	if err != nil {
		return nil, fmt.Errorf("raw video storage save failed: %w", err)
	}
	log.Println("size of the raw file: ", float64(size)/float64(1024*1024))
	// videos/2026/06/video_uuid/master.m3u8
	playlistKey := v.generateStorageStreamKey(videoID)
	playBackURL, err := v.delivery.URL(ctx, playlistKey)
	if err != nil {
		// os.Remove(tempSourcePath)
		return nil, fmt.Errorf("failed to generate playback URL: %w", err)
	}
	job := TranscodeJob{
		VideoID:          videoID,
		StorageSourceKey: rawObjectKey,
	}
	select {
	case v.jobQueue <- job:
		log.Printf("Job %s successfully queued: ", videoID)
	default:
		//queue is full, handle backpressure
		// os.Remove(tempSourcePath)
		return nil, fmt.Errorf("server queue is full, please retry again")
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
	url, err := s.delivery.URL(ctx, objectKey)
	if err != nil {
		return "", fmt.Errorf("failed to get playback URL for key %s: %w", objectKey, err)
	}

	return url, nil
}

// Delete removes a video from the storage provider (and eventually the database).
func (s *VideoService) Delete(ctx context.Context, objectKey string) error {
	err := s.storage.DeleteRaw(ctx, objectKey)
	if err != nil {
		return fmt.Errorf("failed to delete video from storage: %w", err)
	}
	return nil
}
