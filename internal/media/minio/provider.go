package minio

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"

	"github.com/arjun118/fileupload/internal/media"
	"github.com/minio/minio-go/v7"
)

// put object
// remove object
// stat object

type Storage struct {
	Client     *minio.Client
	BucketName string
}

func NewStorage(client *minio.Client, bucketName string) *Storage {
	return &Storage{
		Client:     client,
		BucketName: bucketName,
	}
}

func (s *Storage) EnsureBucket(ctx context.Context) error {
	bucketExists, err := s.Client.BucketExists(ctx, s.BucketName)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !bucketExists {
		log.Printf("bucket doesnot exists, creating bucket: %s\n", s.BucketName)
		err := s.Client.MakeBucket(ctx, s.BucketName, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}
	//you might want to configure cors rules for your bucket while using the backend with a frontend
	return nil
}

func (s *Storage) Save(ctx context.Context, objectKey string, r io.Reader, meta media.FileMetaData) (int64, error) {
	//save this to minio bucket
	// videos/year/month/filename.ext
	filePathWithFolder := filepath.FromSlash(objectKey)

	var contentType string
	switch filepath.Ext(objectKey) {
	case ".m3u8":
		contentType = "application/vnd.apple.mpegurl"
	case ".ts":
		contentType = "video/mp2t"
	case ".vtt":
		contentType = "text/vtt"
	case ".jpg", ".jpeg":
		contentType = "image/jpeg"
	case ".png":
		contentType = "image/png"
	default:
		contentType = "application/octet-stream"
	}

	uploadInfo, err := s.Client.PutObject(ctx, s.BucketName, filePathWithFolder,
		r, -1, minio.PutObjectOptions{
			ContentType: contentType,
		})
	if err != nil {
		return 0, err
	}
	size := uploadInfo.Size

	return size, nil
}

func (s *Storage) Delete(ctx context.Context, objectKey string) error {
	//delete from bucket
	filePathWithFolder := filepath.FromSlash(objectKey)
	bucketExists, err := s.Client.BucketExists(ctx, s.BucketName)
	if err != nil {
		return err
	}
	if bucketExists {
		_, err := s.Client.StatObject(ctx, s.BucketName, filePathWithFolder, minio.StatObjectOptions{})
		if err != nil {
			return err
		}
	}
	err = s.Client.RemoveObject(ctx, s.BucketName, filePathWithFolder, minio.RemoveObjectOptions{})
	return err
}
