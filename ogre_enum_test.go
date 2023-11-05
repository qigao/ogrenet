package ogrenet

import (
	"testing"
)

func TestIsProxyModeValid(t *testing.T) {
	testCases := []struct {
		name  string
		value ProxyMode
		want  bool
	}{
		{
			name:  "valid Push",
			value: Push,
			want:  true,
		},
		{
			name:  "valid Publish",
			value: Publish,
			want:  true,
		},
		{
			name:  "valid Rotate",
			value: Rotate,
			want:  true,
		},
		{
			name:  "invalid value",
			value: ProxyMode(99),
			want:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsProxyModeValid(tc.value)
			if got != tc.want {
				t.Errorf("IsProxyModeValid(%v) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
