package ogrenet

import (
	"testing"
)

func TestIsProxyModeValid(t *testing.T) {
	testCases := []struct {
		name  string
		value WorkMode
		want  bool
	}{
		{
			name:  "valid Push",
			value: PushMode,
			want:  true,
		},
		{
			name:  "valid Publish",
			value: PubMode,
			want:  true,
		},
		{
			name:  "valid Rotate",
			value: LoadBalance,
			want:  true,
		},
		{
			name:  "invalid value",
			value: WorkMode(99),
			want:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsValidWorkMode(tc.value)
			if got != tc.want {
				t.Errorf("IsProxyModeValid(%v) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
