package jobs

type TranscodeJob struct {
	VideoID string `json:"videoid"`
	//this will be the object key for the source video
	// need to download from this
	StorageSourceKey string `json:"source_key"`
}
