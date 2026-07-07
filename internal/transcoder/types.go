package transcoder

import "time"

type ProbeResult struct {
	Width    int
	Height   int
	FPS      float64
	HasAudio bool
	Duration time.Duration
}

type Rendition struct {
	Width        int
	Height       int
	VideoBitrate int
	MaxRate      int
	BufSize      int
	AudioBitRate int
}

type EncodingPlan struct {
	InputFile       string
	OutputDir       string
	GOP             int
	SegmentDuration int

	Renditions []Rendition

	Probe        ProbeResult
	ManifestName string
	MasterName   string
	HasAudio     bool
}

var DefaultRenditions = []Rendition{
	{
		Width:        640,
		Height:       360,
		VideoBitrate: 800,
		MaxRate:      856,
		BufSize:      1200,
		AudioBitRate: 96,
	},
	{
		Width:        1280,
		Height:       720,
		VideoBitrate: 2500,
		MaxRate:      2675,
		BufSize:      3750,
		AudioBitRate: 128,
	},
	{
		Width:        1920,
		Height:       1080,
		VideoBitrate: 5000,
		MaxRate:      5350,
		BufSize:      7500,
		AudioBitRate: 128,
	},
}
