package operation

func dupStrings(s []string) []string {
	if len(s) == 0 {
		return s
	}
	to := make([]string, len(s))
	copy(to, s)
	return to
}
