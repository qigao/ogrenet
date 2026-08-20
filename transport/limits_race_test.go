package transport

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestAdmissionConcurrentAcquireRelease(t *testing.T) {
	const limit = 8
	a := newAdmissionController(Limits{MaxConnections: limit})
	var maxSeen atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lease, err := a.acquireConnection("")
				if err != nil {
					if !errors.Is(err, ErrResourceExhausted) {
						t.Errorf("acquire: %v", err)
					}
					continue
				}
				active := int64(a.snapshot().ActiveConnections)
				for {
					old := maxSeen.Load()
					if active <= old || maxSeen.CompareAndSwap(old, active) {
						break
					}
				}
				lease.release()
			}
		}()
	}
	wg.Wait()
	if got := maxSeen.Load(); got > limit {
		t.Fatalf("max active = %d, limit = %d", got, limit)
	}
	if got := a.snapshot().ActiveConnections; got != 0 {
		t.Fatalf("final active = %d, want 0", got)
	}
}
