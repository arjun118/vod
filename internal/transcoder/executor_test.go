package transcoder

import (
	"context"
	"os"
	"path"
	"testing"
	"time"
)

func TestFFProbe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	homeDir, _ := os.UserHomeDir()
	res, err := execProber(ctx, []string{
		"-v", "error",
		"-show_streams",
		"-show_format",
		"-print_format", "json",
	}, path.Join(homeDir, "temp", "has_audio.mp4"))
	if err != nil {
		t.Fatalf("execProber failed: %v", err)
	}
	if len(res.Streams) == 0 {
		t.Fatal("expected at least one stream")
	}

	if res.Format.Duration == "" {
		t.Fatal("expected duration to be present")
	}

	t.Logf("Duration: %s", res.Format.Duration)

	for _, stream := range res.Streams {
		t.Logf(
			"Codec=%s Width=%d Height=%d FPS=%s",
			stream.CodecType,
			stream.Width,
			stream.Height,
			stream.AvgFrameRate,
		)
	}
}
