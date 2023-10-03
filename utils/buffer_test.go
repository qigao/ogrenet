package utils

import (
	"bytes"
	"reflect"
	"testing"
)

const NewLine = "\r\n"

func TestBytesSplit(t *testing.T) {
	testCases := []struct {
		name     string
		input    []byte
		expected [][]byte
	}{
		{
			name:     "single line",
			input:    []byte("hello world\r\n"),
			expected: [][]byte{[]byte("hello world")},
		},
		{
			name:     "multiple lines",
			input:    []byte("hello\r\nworld\r\n"),
			expected: [][]byte{[]byte("hello"), []byte("world")},
		},
		{
			name:     "multiple lines",
			input:    []byte("hello\r\nworld \r\n"),
			expected: [][]byte{[]byte("hello"), []byte("world")},
		},
		{
			name:     "empty input",
			input:    []byte(""),
			expected: [][]byte{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := SplitSliceBySep(tc.input, []byte(NewLine))
			if !reflect.DeepEqual(actual, tc.expected) {
				t.Errorf("expected %v, but got %v", tc.expected, actual)
			}
		})
	}
}

func TestEmptyByteSlice(t *testing.T) {
	testCases := []struct {
		name     string
		input    []byte
		expected bool
	}{
		{
			name:     "empty byte slice",
			input:    []byte(""),
			expected: true,
		},
		{
			name:     "not empty byte slice",
			input:    []byte("hello world"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := len(tc.input) == 0
			if actual != tc.expected {
				t.Errorf("expected %v, but got %v", tc.expected, actual)
			}
		})
	}
}

func TestTrimSliceSuffix(t *testing.T) {
	testCases := []struct {
		name   string
		input  []byte
		suffix string
		want   []byte
	}{
		{
			name:   "empty input",
			input:  []byte{},
			suffix: "suffix",
			want:   []byte{},
		},
		{
			name:   "suffix not found",
			input:  []byte("hello world"),
			suffix: "suffix",
			want:   []byte("hello world"),
		},
		{
			name:   "suffix found",
			input:  []byte("hello worldsuffix"),
			suffix: "suffix",
			want:   []byte("hello world"),
		},
		{
			name:   "suffix found multiple times",
			input:  []byte("hello worldsuffixsuffixsuffix"),
			suffix: "suffix",
			want:   []byte("hello world"),
		},
		{
			name:   "suffix at beginning",
			input:  []byte("suffixhello world"),
			suffix: "suffix",
			want:   []byte("hello world"),
		},
		{
			name:   "suffix at end",
			input:  []byte("hello worldsuffix"),
			suffix: "suffix",
			want:   []byte("hello world"),
		},
		{
			name:   "suffix is input",
			input:  []byte("suffix"),
			suffix: "suffix",
			want:   []byte{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := trimSliceSuffix(tc.input, tc.suffix)
			if !bytes.Equal(got, tc.want) {
				t.Errorf("trimSliceSuffix(%q, %q) = %q; want %q", tc.input, tc.suffix, got, tc.want)
			}
		})
	}
}
