//go:build linux

package transport

import (
	"sync/atomic"
	"testing"
)

func TestEpollReactorWorkerCapacityHandshakeCannotLoseRelease(t *testing.T) {
	r := newTestEpollReactor(t)
	res := newTestEventResource(11, -1, 1)
	if err := r.registerResource(res); err != nil {
		t.Fatal(err)
	}
	var capacity atomic.Bool
	capacity.Store(true) // release happened before blocked registration became visible
	r.workerCapacityAvailable = capacity.Load

	r.blockOnWorker(res)
	if !r.hasWorkerBlocked.Load() {
		t.Fatal("worker-blocked resource was not published")
	}
	r.drainControl()
	r.drainRunnable()
	if got := res.runs.Load(); got != 1 {
		t.Fatalf("capacity release was lost: runs=%d, want 1", got)
	}
	if r.hasWorkerBlocked.Load() {
		t.Fatal("worker-blocked flag remained set after retry")
	}
}
