package transcoder

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
)

type ErrorSeverity int

const (
	SeverityTransient ErrorSeverity = iota // Retry: network timeout, disk full, MinIO down
	SeverityPermanent                      // Don't retry: corrupt file, unsupported codec
)

type TranscodeError struct {
	Err    error
	Stderr string
}

func (e *TranscodeError) Error() string {
	return e.Err.Error()
}

func (e *TranscodeError) Unwrap() error {
	return e.Err
}

func ClassifyError(err error, stderr string) ErrorSeverity {
	if errors.Is(err, context.DeadlineExceeded) {
		return SeverityTransient
	}

	if errors.Is(err, os.ErrNotExist) {
		// source file disappeared; usually transient in distributed systems
		return SeverityTransient
	}

	// network timeout example
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return SeverityTransient
	}

	permanentPatterns := []string{
		"invalid data found when processing input",
		"decoder not found",
		"unsupported codec",
	}

	for _, p := range permanentPatterns {
		if strings.Contains(strings.ToLower(stderr), p) {
			return SeverityPermanent
		}
	}

	return SeverityTransient
}
