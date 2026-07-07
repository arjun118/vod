package transcoder

import (
	"context"
	"fmt"
	"os"
)

type Transcoder struct {
	Renditions []Rendition
}

func New() *Transcoder {
	return &Transcoder{
		Renditions: DefaultRenditions,
	}
}

func (t *Transcoder) Transcode(
	ctx context.Context,
	input string,
	output string,
) error {

	if err := os.MkdirAll(output, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	probe, err := Probe(input)
	if err != nil {
		return fmt.Errorf("probe input: %w", err)
	}

	plan := BuildPlan(
		probe,
		input,
		output,
		t.Renditions,
	)

	return Run(ctx, &plan)
}
