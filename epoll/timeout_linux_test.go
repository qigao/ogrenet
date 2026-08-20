//go:build linux

package epoll

import (
	"math"
	"testing"
	"time"
)

func TestTimeoutMillisMaxDuration(t *testing.T) {
	got := timeoutMillis(time.Duration(math.MaxInt64), 0)
	if got != math.MaxInt32 {
		t.Fatalf("timeoutMillis(max duration, 0) = %d, want %d", got, math.MaxInt32)
	}
}
