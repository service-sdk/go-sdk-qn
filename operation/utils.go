package operation

import (
	"strings"
	"time"
)

func dupStrings(s []string) []string {
	if len(s) == 0 {
		return s
	}
	to := make([]string, len(s))
	copy(to, s)
	return to
}

func makeSureKeyAsDir(key string) string {
	if strings.HasSuffix(key, "/") {
		return key
	}
	return key + "/"
}

// buildDurationByMs build time.Duration by ms, if ms <= 0, return defaultValue
func buildDurationByMs(ms int, defaultValue int) time.Duration {
	if ms <= 0 {
		return time.Duration(defaultValue) * time.Millisecond
	}
	return time.Duration(ms) * time.Millisecond
}
