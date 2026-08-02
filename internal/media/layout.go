package media

import (
	"path"
	"path/filepath"
	"time"
)

type StorageLayout struct {
	prefix string //videos
}

func (s *StorageLayout) ForVideo(videoID string) VideoKeys {
	//path because these are storage keys not local file paths
	now := time.Now()
	base := path.Join(s.prefix, now.Format("2006"), now.Format("01"), videoID)
	return VideoKeys{
		RawObjectKey: path.Join(base, "raw.mp4"),
		StreamFolder: base,
		PlaylistKey:  path.Join(base, "master.m3u8"),
	}
}

func NewStorageLayout(prefix string) *StorageLayout {
	return &StorageLayout{
		prefix: prefix,
	}
}

type VideoKeys struct {
	RawObjectKey string // videos/2026/08/uuid/raw.mp4
	StreamFolder string // videos/2026/08/uuid
	PlaylistKey  string // videos/2026/08/uuid/master.m3u8
}

func (k VideoKeys) StreamObjectKey(filename string) string {
	return filepath.Join(k.StreamFolder, filename)
}
