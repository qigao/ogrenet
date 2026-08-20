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

func pkcs7Pad(src []byte, blockSize int) []byte {
	padding := blockSize - len(src)%blockSize
	out := make([]byte, len(src)+padding)
	copy(out, src)
	for i := len(src); i < len(out); i++ {
		out[i] = byte(padding)
	}
	return out
}

func pkcs7Unpad(src []byte, blockSize int) ([]byte, bool) {
	if len(src) == 0 || len(src)%blockSize != 0 {
		return nil, false
	}
	padding := int(src[len(src)-1])
	if padding == 0 || padding > blockSize || padding > len(src) {
		return nil, false
	}
	for _, b := range src[len(src)-padding:] {
		if int(b) != padding {
			return nil, false
		}
	}
	return src[:len(src)-padding], true
}
