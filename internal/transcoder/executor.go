package transcoder

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
)

type ffprobeOutput struct {
	Streams []struct {
		CodeName     string `json:"code_name"`
		CodecType    string `json:"codec_type"`
		SampleRate   string `json:"sample_rate"` //for audio
		BitRate      string `json:"bit_rate"`    // intented for audio but lets see
		Width        int    `json:"width"`
		Height       int    `json:"height"`
		AvgFrameRate string `json:"avg_frame_rate"`
	} `json:"streams"`

	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

func execProber(ctx context.Context, args []string, filePath string) (ffprobeOutput, error) {
	args = append(args, filePath)

	cmd := exec.CommandContext(ctx, "ffprobe", args...)

	out, err := cmd.Output()
	if err != nil {
		return ffprobeOutput{}, fmt.Errorf("ffprobe failed: %w", err)
	}

	var result ffprobeOutput
	if err := json.Unmarshal(out, &result); err != nil {
		return ffprobeOutput{}, fmt.Errorf("failed to parse ffprobe output: %w", err)
	}

	return result, nil
}
