package transcoder

import (
	"strconv"
	"strings"
)

func parseFPS(rate string) float64 {
	parts := strings.Split(rate, "/")
	if len(parts) != 2 {
		return 0
	}

	num, err1 := strconv.ParseFloat(parts[0], 64)
	den, err2 := strconv.ParseFloat(parts[1], 64)

	if err1 != nil || err2 != nil || den == 0 {
		return 0
	}

	return num / den
}
