package legacy

func normalizeFixed(src []byte, n int) []byte {
	out := make([]byte, n)
	copy(out, src)
	if len(src) < n {
		for i := len(src); i < n; i++ {
			out[i] = ' '
		}
	}
	return out
}
