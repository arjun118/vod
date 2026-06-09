package service

//////fix this shit later
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
		storage:  storage,
		delivery: delivery,
		//buffer size for queued jobs
		jobQueue:   make(chan TranscodeJob, 100),
		maxWorkers: maxWorkers,
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
				log.Printf("Worker %d starting job %s", workerID, job.ID)
				jobCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				err := v.processAndSaveHLS(jobCtx, job.ID, job.TempSourcePath, job.FinalBasePath)
				if err != nil {
					log.Printf("[Error] Job %s failed: %v", job.ID, err)
					//prod: update database job status - failed
				} else {
					log.Printf("Job %s completed successfully", job.ID)
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

func (v *VideoService) generateHLSFolderPath(videoID string) string {
	now := time.Now()
	year := now.Format("2006")
	month := now.Format("01")

	return filepath.ToSlash(filepath.Join("videos", year, month, videoID))
}

func (v *VideoService) processAndSaveHLS(ctx context.Context, videoID string, tempSourcePath string, finalBasePath string) (err error) {
	tempHLSDir := filepath.Join("/tmp", "hls_"+videoID)

	if err := os.MkdirAll(tempHLSDir, 0755); err != nil {
		return fmt.Errorf("failed to create HLS temp dir: %w", err)
	}
	//clean the temporary folders
	defer func() {
		os.RemoveAll(tempHLSDir)
		os.Remove(tempSourcePath)
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered during transcoding: %v", r)
		}
	}()

	_, err = TranscodeToHLS(ctx, tempSourcePath, tempHLSDir)
	if err != nil {
		return fmt.Errorf("transcode failed: %w", err)
	}
	entries, err := os.ReadDir(tempHLSDir)
	if err != nil {
		return fmt.Errorf("failed to read output directory %w", err)
	}
	for _, entry := range entries {

		if entry.IsDir() {
			continue
		}
		//playlist.m3u8 or playlist0.ts
		fileName := entry.Name()
		tempFilePath := filepath.Join(tempHLSDir, fileName)
		finalObjectKey := filepath.ToSlash(filepath.Join(finalBasePath, fileName))
		if err := v.saveSegment(ctx, tempFilePath, finalObjectKey, fileName); err != nil {
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

func (v *VideoService) Upload(ctx context.Context, file io.Reader, meta media.FileMetaData) (*media.MediaObject, error) {
	//a uuid string
	videoID := uuid.NewString()
	// save the raw file to the disk right now
	tempRawDir := filepath.Join("/tmp", "raw_videos")
	if err := os.MkdirAll(tempRawDir, 0755); err != nil {
		return nil, err
	}
	tempSourcePath := filepath.Join(tempRawDir, videoID+".mp4")
	tempFile, err := os.Create(tempSourcePath)
	if err != nil {
		return nil, err
	}
	log.Println("writing the raw file to temp file: ", tempSourcePath)
	size, err := io.Copy(tempFile, file)
	log.Println("size of the raw file: ", float64(size)/float64(1024*1024))
	tempFile.Close()
	if err != nil {
		os.Remove(tempSourcePath)
		return nil, fmt.Errorf("failed to save raw file upload: %w", err)
	}
	// videos/2026/06/video_uuid
	finalBasePath := v.generateHLSFolderPath(videoID)
	// videos/2026/06/video_uuid/playlist.m3u8
	playlistKey := filepath.ToSlash(filepath.Join(finalBasePath, "playlist.m3u8"))
	playBackURL, err := v.delivery.URL(ctx, playlistKey)
	if err != nil {
		os.Remove(tempSourcePath)
		return nil, fmt.Errorf("failed to generate playback URL: %w", err)
	}
	job := TranscodeJob{
		ID:             videoID,
		TempSourcePath: tempSourcePath,
		FinalBasePath:  finalBasePath,
		Ctx:            context.Background(),
	}
	select {
	case v.jobQueue <- job:
		log.Printf("Job %s successfully queued: ", videoID)
	default:
		//queue is full, handle backpressure
		os.Remove(tempSourcePath)
		return nil, fmt.Errorf("server queue is full, please retry again")
	}
	return &media.MediaObject{
		ID:          videoID,
		ObjectKey:   playlistKey,
		Provider:    "local",
		ContentType: "application/vnd.apple.mpegurl",
		Size:        size,
		PlaybackURL: playBackURL,
	}, nil
}

func (s *VideoService) GetPlaybackURL(ctx context.Context, objectKey string) (string, error) {
	// Business logic goes here:
	// e.g., s.db.GetVideoOwner(objectKey) -> check if current user is allowed to view it.

	url, err := s.delivery.URL(ctx, objectKey)
	if err != nil {
		return "", fmt.Errorf("failed to get playback URL for key %s: %w", objectKey, err)
	}

	return url, nil
}

// Delete removes a video from the storage provider (and eventually the database).
func (s *VideoService) Delete(ctx context.Context, objectKey string) error {
	// Business logic goes here:
	// e.g., check if the user requesting the delete actually owns the video.

	// 1. Delete from physical storage
	err := s.storage.Delete(ctx, objectKey)
	if err != nil {
		return fmt.Errorf("failed to delete video from storage: %w", err)
	}

	// 2. Future: Delete metadata from database
	// err = s.db.DeleteVideo(ctx, objectKey)
	// if err != nil { ... }

	return nil
}
