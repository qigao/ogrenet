package utils

import (
	"bytes"
)

func SplitSliceBySep(b []byte, sep []byte) [][]byte {
	tmp := bytes.Split(b, sep)
	result := make([][]byte, 0, len(tmp))
	for _, v := range tmp {
		if len(v) != 0 {
			trimSlice := bytes.TrimSpace(v)
			result = append(result, trimSlice)
		}
	}
	return result
}

func trimSliceSuffix(b []byte, suffix string) []byte {
	return bytes.ReplaceAll(b, []byte(suffix), []byte{})
}
