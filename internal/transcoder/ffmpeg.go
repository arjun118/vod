package transcoder

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func Probe(inputFilePath string) (ProbeResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	probeOutput, err := execProber(ctx, []string{
		"-v", "error",
		"-show_streams",
		"-show_format",
		"-print_format", "json",
	}, inputFilePath)
	if err != nil {
		return ProbeResult{}, fmt.Errorf("failed to probe metadata: %w", err)
	}

	var result ProbeResult

	for _, stream := range probeOutput.Streams {
		switch stream.CodecType {
		case "video":
			result.Width = stream.Width
			result.Height = stream.Height
			result.FPS = parseFPS(stream.AvgFrameRate)
		case "audio":
			result.HasAudio = true
		}
	}
	if probeOutput.Format.Duration != "" {
		seconds, err := strconv.ParseFloat(probeOutput.Format.Duration, 64)
		if err == nil {
			result.Duration = time.Duration(seconds * float64(time.Second))
		}
	}
	return result, nil
}

func BuildPlan(
	probe ProbeResult,
	input string,
	output string,
	currentRenditions []Rendition,
) EncodingPlan {

	const segmentDuration = 3

	gop := int(math.Round(probe.FPS * segmentDuration))
	if gop <= 0 {
		gop = 90
	}

	var renditions []Rendition

	for _, r := range currentRenditions {
		if r.Height <= probe.Height {
			renditions = append(renditions, r)
		}
	}
	if len(renditions) == 0 {
		renditions = append(renditions, currentRenditions[0])
	}

	return EncodingPlan{
		InputFile:       input,
		OutputDir:       output,
		SegmentDuration: segmentDuration,
		GOP:             gop,
		Probe:           probe,
		Renditions:      renditions,
		ManifestName:    "manifest.mpd",
		MasterName:      "master.m3u8",
		HasAudio:        probe.HasAudio,
	}
}

func BuildArgs(plan *EncodingPlan) []string {
	args := []string{
		"-y",
		"-i", plan.InputFile,
	}

	//
	// Build filter graph
	//

	var filter strings.Builder

	fmt.Fprintf(&filter, "[0:v]split=%d", len(plan.Renditions))

	for i := range plan.Renditions {
		fmt.Fprintf(&filter, "[v%d]", i)
	}

	filter.WriteString(";")

	for i, r := range plan.Renditions {
		fmt.Fprintf(
			&filter,
			"[v%d]"+
				"scale=w=%d:h=%d:"+
				"force_original_aspect_ratio=decrease:"+
				"force_divisible_by=2,"+
				"pad=%d:%d:(ow-iw)/2:(oh-ih)/2"+
				"[v%dout];",
			i,
			r.Width,
			r.Height,
			r.Width,
			r.Height,
			i,
		)
	}

	filterGraph := strings.TrimSuffix(filter.String(), ";")

	args = append(args,
		"-filter_complex", filterGraph,
	)

	//
	// Stream mappings
	//

	for i := range plan.Renditions {

		args = append(args,
			"-map", fmt.Sprintf("[v%dout]", i),
		)

		if plan.HasAudio {
			args = append(args,
				"-map", "0:a:0",
			)
		}
	}

	//
	// Video encoder
	//
	for i := range plan.Renditions {
		args = append(args,
			fmt.Sprintf("-c:v:%d", i),
			"libx264",
		)
	}
	args = append(args,
		"-threads", "3",
		"-preset", "medium",
		"-profile:v", "high",
		"-pix_fmt", "yuv420p",
		"-movflags", "+frag_keyframe+empty_moov",
		"-g", strconv.Itoa(plan.GOP),
		"-keyint_min", strconv.Itoa(plan.GOP),
		"-sc_threshold", "0",
	)

	//
	// Per rendition settings
	//

	for i, r := range plan.Renditions {

		args = append(args,
			fmt.Sprintf("-b:v:%d", i), fmt.Sprintf("%dk", r.VideoBitrate),
			fmt.Sprintf("-maxrate:v:%d", i), fmt.Sprintf("%dk", r.MaxRate),
			fmt.Sprintf("-bufsize:v:%d", i), fmt.Sprintf("%dk", r.BufSize),
		)
	}

	//
	// Audio
	//

	if plan.HasAudio {

		for i := range plan.Renditions {
			args = append(args,
				fmt.Sprintf("-c:a:%d", i),
				"aac",
			)
		}
		args = append(args,
			"-ar", "48000",
			"-ac", "2",
		)

		for i, r := range plan.Renditions {

			args = append(args,

				fmt.Sprintf("-b:a:%d", i),
				fmt.Sprintf("%dk", r.AudioBitRate),
			)
		}
	}

	//
	// DASH
	//

	args = append(args,

		"-f", "dash",

		"-seg_duration",
		strconv.Itoa(plan.SegmentDuration),

		"-use_template", "1",

		"-use_timeline", "1",
	)

	if plan.HasAudio {

		args = append(args,
			"-adaptation_sets",
			"id=0,streams=v id=1,streams=a",
		)

	} else {

		args = append(args,
			"-adaptation_sets",
			"id=0,streams=v",
		)
	}

	args = append(args,

		"-init_seg_name",
		"init-$RepresentationID$.$ext$",

		"-media_seg_name",
		"chunk-$RepresentationID$-$Number%05d$.$ext$",

		"-hls_playlist", "1",

		"-hls_master_name", plan.MasterName,

		filepath.Join(plan.OutputDir, plan.ManifestName),
	)

	return args
}

func CommandString(args []string) string {
	return "ffmpeg " + strings.Join(args, " ")
}

func Run(ctx context.Context, plan *EncodingPlan) error {

	args := BuildArgs(plan)

	cmd := exec.CommandContext(
		ctx,
		"ffmpeg",
		args...,
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return &TranscodeError{
			Err:    err,
			Stderr: stderr.String(),
		}
	}

	return nil
}
