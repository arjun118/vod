package local

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/arjun118/fileupload/internal/media"
)

type Storage struct {
	RootDir string
}

func NewStorage(rootDIR string) *Storage {
	return &Storage{
		RootDir: rootDIR,
	}
}

func (p *Storage) Save(ctx context.Context, objectKey string, r io.Reader, meta media.FileMetaData) (int64, error) {
	fullPath := filepath.Join(p.RootDir, filepath.FromSlash(objectKey))

	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return 0, err
	}
	f, err := os.Create(fullPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	size, err := io.Copy(f, r)
	if err != nil {
		return 0, err
	}

	// return &media.MediaObject{
	// 	ID:          filepath.Base(objectKey), // Or whatever the service decides
	// 	ObjectKey:   objectKey,
	// 	Provider:    "local",
	// 	ContentType: meta.ContentType,
	// 	Size:        size,
	// }, nil
	return size, nil
}

func (p *Storage) Delete(ctx context.Context, objectKey string) error {
	fullPath := filepath.Join(p.RootDir, filepath.FromSlash(objectKey))
	if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
