package logger

import (
	"io"

	"github.com/rs/zerolog"
)

func New(out io.Writer) zerolog.Logger {
	return zerolog.New(out).With().Timestamp().Logger()
}
