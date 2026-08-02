package minio

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

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
		log.Println("setting stream bucket to public - only accessible over the docker network not by all the internet")
		publicPolicy := fmt.Sprintf(`{
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Allow",
							"Principal": {"AWS": ["*"]},
							"Action": ["s3:GetObject"],
							"Resource": ["arn:aws:s3:::%s/*"]
						}
					]
			}`, s.streamBucketName)
		err = s.Client.SetBucketPolicy(ctx, s.streamBucketName, publicPolicy)
		if err != nil {
			return fmt.Errorf("failed to set streams bucket as public: %w", err)
		}
		log.Printf("streams bucket made public successfully...")
	}
	return nil
}

func (s *Storage) SaveRaw(ctx context.Context, objectKey string, r io.Reader) (int64, error) {
	//save this to minio bucket
	// 	// videos/year/month/filename.ext
	// this will just upload to the raw bucket
	contentType := media.ContentTypeFromExtension(objectKey)
	uploadInfo, err := s.Client.PutObject(ctx, s.rawVideosBucket, objectKey,
		r, -1, minio.PutObjectOptions{
			ContentType: contentType,
		})
	if err != nil {
		return 0, err
	}
	size := uploadInfo.Size

	return size, nil
}

func (s *Storage) SaveStream(ctx context.Context, objectKey string, r io.Reader) (int64, error) {
	//save this to minio bucket
	// 	// videos/year/month/filename.ext
	// this will just upload to the stream bucket
	contentType := media.ContentTypeFromExtension(objectKey)
	uploadInfo, err := s.Client.PutObject(ctx, s.streamBucketName, objectKey,
		r, -1, minio.PutObjectOptions{
			ContentType: contentType,
		})
	if err != nil {
		return 0, err
	}
	size := uploadInfo.Size

	return size, nil
}

func (s *Storage) DeleteRaw(ctx context.Context, objectKey string) error {
	//delete from raw videos
	filePathWithFolder := filepath.FromSlash(objectKey)
	bucketExists, err := s.Client.BucketExists(ctx, s.rawVideosBucket)
	if err != nil {
		return err
	}
	if bucketExists {
		_, err := s.Client.StatObject(ctx, s.rawVideosBucket, filePathWithFolder, minio.StatObjectOptions{})
		if err != nil {
			return err
		}
	}
	err = s.Client.RemoveObject(ctx, s.rawVideosBucket, filePathWithFolder, minio.RemoveObjectOptions{})
	return err
}

func (s *Storage) DeleteStream(ctx context.Context, objectKey string) error {
	//delete from stream bucket
	filePathWithFolder := filepath.FromSlash(objectKey)
	bucketExists, err := s.Client.BucketExists(ctx, s.streamBucketName)
	if err != nil {
		return err
	}
	if bucketExists {
		_, err := s.Client.StatObject(ctx, s.streamBucketName, filePathWithFolder, minio.StatObjectOptions{})
		if err != nil {
			return err
		}
	}
	err = s.Client.RemoveObject(ctx, s.streamBucketName, filePathWithFolder, minio.RemoveObjectOptions{})
	return err
}

func (s *Storage) Get(ctx context.Context, objectKey string, destinationDir string) (string, error) {
	minioObject, err := s.Client.GetObject(ctx, s.rawVideosBucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get the raw video: %w", err)
	}

	defer minioObject.Close()
	if err := os.MkdirAll(destinationDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create HLS temp dir: %w", err)
	}
	destinationFilePath := filepath.Join(destinationDir, filepath.Base(objectKey))
	localFile, err := os.Create(destinationFilePath)
	if err != nil {
		return "", fmt.Errorf("could not able to create local file for raw video download: %w", err)
	}
	defer localFile.Close()
	if _, err = io.Copy(localFile, minioObject); err != nil {
		return "", fmt.Errorf("could not raw download video from storage: %w", err)
	}
	return destinationFilePath, nil
}
