package operation

import "strings"

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
