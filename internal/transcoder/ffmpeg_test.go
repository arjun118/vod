package transcoder

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildPlan(t *testing.T) {
	// homeDir, _ := os.UserHomeDir()
	// filePath := path.Join(homeDir, "tmp", "vid1.mp4")
	probe := ProbeResult{
		Width:    1920,
		Height:   1080,
		FPS:      30,
		HasAudio: true,
	}

	plan := BuildPlan(probe, "input.mp4", "/tmp/output", DefaultRenditions)

	if len(plan.Renditions) != 3 {
		t.Fatalf("expected 3 renditions, got %d", len(plan.Renditions))
	}

	if !plan.HasAudio {
		t.Fatal("expected audio")
	}

	if plan.GOP != 90 {
		t.Fatalf("expected GOP 90, got %d", plan.GOP)
	}
}

func TestBuildArgs(t *testing.T) {
	plan := &EncodingPlan{
		InputFile: "vid.mp4",
		OutputDir: "/tmp/output",

		SegmentDuration: 3,
		GOP:             90,
		HasAudio:        true,

		Renditions: []Rendition{
			{
				Width:        640,
				Height:       360,
				VideoBitrate: 800,
				MaxRate:      856,
				BufSize:      1200,
				AudioBitRate: 96,
			},
		},
		MasterName:   "master.m3u8",
		ManifestName: "manifest.mpd",
	}

	args := BuildArgs(plan)
	t.Log(CommandString(args))
	require.Contains(t, args, "-filter_complex")
	require.Contains(t, args, "-hls_playlist")
	require.Contains(t, args, "-adaptation_sets")
	require.Contains(t, args, "id=0,streams=v id=1,streams=a")
}

func TestProbe_HasAudio(t *testing.T) {
	probe, err := Probe("testdata/has_audio.mp4")
	require.NoError(t, err)

	require.True(t, probe.HasAudio)
	require.Equal(t, 1920, probe.Width)
}

func TestRun(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}

	input := "testdata/has_audio.mp4"

	output := t.TempDir()

	probe, err := Probe(input)
	require.NoError(t, err)

	plan := BuildPlan(probe, input, output, DefaultRenditions)

	err = Run(context.Background(), &plan)
	require.NoError(t, err)

	require.FileExists(t, filepath.Join(output, "manifest.mpd"))
	require.FileExists(t, filepath.Join(output, "master.m3u8"))
}
