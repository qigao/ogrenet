//go:build windows

package iocp

import (
	"math"
	"testing"
	"time"
)

func TestTimeoutMillisMaxDuration(t *testing.T) {
	got := timeoutMillis(time.Duration(math.MaxInt64))
	want := uint32(math.MaxUint32 - 1)
	if got != want {
		t.Fatalf("timeoutMillis(max duration) = %d, want %d", got, want)
	}
}
