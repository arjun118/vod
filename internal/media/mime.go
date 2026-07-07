package media

import "path/filepath"

func ContentTypeFromExtension(objectKey string) string {
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
	return contentType
}
