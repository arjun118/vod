package minio

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/arjun118/fileupload/internal/media"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

// put object
// remove object
// stat object

type Storage struct {
	Client           *minio.Client
	rawVideosBucket  string
	streamBucketName string
}

func NewStorage(client *minio.Client, rawBucketName, streamBucketName string) *Storage {
	return &Storage{
		Client:           client,
		rawVideosBucket:  rawBucketName,
		streamBucketName: streamBucketName,
	}
}

func (s *Storage) EnsureBuckets(ctx context.Context) error {
	if err := s.ensureRawBucket(ctx); err != nil {
		return err
	}
	if err := s.ensureStreamBucket(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Storage) ensureRawBucket(ctx context.Context) error {
	bucketExists, err := s.Client.BucketExists(ctx, s.rawVideosBucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !bucketExists {
		log.Printf("bucket doesnot exists, creating bucket: %s\n", s.rawVideosBucket)
		err := s.Client.MakeBucket(ctx, s.rawVideosBucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		config := lifecycle.NewConfiguration()
		config.Rules = []lifecycle.Rule{
			{
				ID:     "ExpireRawUploads",
				Status: "Enabled",
				Expiration: lifecycle.Expiration{
					Days: 7,
				},
			},
		}
		lifecycle.NewConfiguration()
		log.Println("Configuring expiry (cleanup) policy for bucket:", s.rawVideosBucket)
		if err = s.Client.SetBucketLifecycle(ctx, s.rawVideosBucket, config); err !=
			nil {
			return fmt.Errorf("failed to set expire policy raw bucket: %w", err)
		}
	}
	return nil
}

func (s *Storage) ensureStreamBucket(ctx context.Context) error {
	bucketExists, err := s.Client.BucketExists(ctx, s.streamBucketName)
	if err != nil {
		return fmt.Errorf("failed to check stream bucket existence: %w", err)
	}
	if !bucketExists {
		log.Printf("bucket doesnot exists, creating bucket: %s\n", s.streamBucketName)
		err := s.Client.MakeBucket(ctx, s.streamBucketName, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}
	return nil
}

func (s *Storage) SaveRaw(ctx context.Context, objectKey string, r io.Reader, meta media.FileMetaData) (int64, error) {

}

func (s *Storage) SaveStream(ctx context.Context, objectKey string, r io.Reader, meta media.FileMetaData) (int64, error) {

}

// func (s *Storage) Save(ctx context.Context, objectKey string, r io.Reader, meta media.FileMetaData) (int64, error) {
// 	//save this to minio bucket
// 	// videos/year/month/filename.ext
// 	filePathWithFolder := filepath.FromSlash(objectKey)

// 	var contentType string
// 	switch filepath.Ext(objectKey) {
// 	case ".m3u8":
// 		contentType = "application/vnd.apple.mpegurl"
// 	case ".ts":
// 		contentType = "video/mp2t"
// 	case ".vtt":
// 		contentType = "text/vtt"
// 	case ".jpg", ".jpeg":
// 		contentType = "image/jpeg"
// 	case ".png":
// 		contentType = "image/png"
// 	default:
// 		contentType = "application/octet-stream"
// 	}

// 	uploadInfo, err := s.Client.PutObject(ctx, s.BucketName, filePathWithFolder,
// 		r, -1, minio.PutObjectOptions{
// 			ContentType: contentType,
// 		})
// 	if err != nil {
// 		return 0, err
// 	}
// 	size := uploadInfo.Size

// 	return size, nil
// }

// func (s *Storage) Delete(ctx context.Context, objectKey string) error {
// 	//delete from bucket
// 	filePathWithFolder := filepath.FromSlash(objectKey)
// 	bucketExists, err := s.Client.BucketExists(ctx, s.BucketName)
// 	if err != nil {
// 		return err
// 	}
// 	if bucketExists {
// 		_, err := s.Client.StatObject(ctx, s.BucketName, filePathWithFolder, minio.StatObjectOptions{})
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	err = s.Client.RemoveObject(ctx, s.BucketName, filePathWithFolder, minio.RemoveObjectOptions{})
// 	return err
// }
