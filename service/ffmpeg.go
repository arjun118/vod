package service

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

func TranscodeToHLS(ctx context.Context, sourceFile string, outputDir string) (string, error) {
	playlistPath := filepath.Join(outputDir, "playlist.m3u8")

	args := []string{"-i", sourceFile}
	canCopy, err := isH264(ctx, sourceFile)
	if err != nil {
		canCopy = false
	}
	//for now
	canCopy = false
	if canCopy {
		args = append(args, "-codec:v", "copy", "-codec:a", "copy")
	} else {
		args = append(args,
			"-codec:v", "libx264",
			"-codec:a", "aac",
			// Add the scale filter to force even dimensions
			"-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
		)
	}
	args = append(args, "-hls_time", "10",
		"-hls_list_size", "0",
		"-f", "hls", playlistPath)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffmpeg failed: %v,output: %s", err, string(output))
	}
	return playlistPath, nil
}

func isH264(ctx context.Context, filePath string) (bool, error) {
	// The ffprobe command to extract just the video codec name
	// -v error: Hide warnings and banner
	// -select_streams v:0: Only look at the first video track
	// -show_entries stream=codec_name: Only print the codec name
	// -of default=...: Format output as plain text with no keys (just the value)
	cmd := exec.CommandContext(
		ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "stream=codec_name",
		"-of", "default=noprint_wrappers=1:nokey=1",
		filePath,
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return false, fmt.Errorf("ffprobe failed: %w", err)
	}
	codec := strings.TrimSpace(out.String())
	return codec == "h264", nil
}
