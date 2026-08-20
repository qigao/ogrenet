package transport

import (
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdmissionStressSlowHandshakeDoesNotBlockConnectionAdmission(t *testing.T) {
	a := newAdmissionController(Limits{
		MaxConnections:          8,
		MaxConcurrentHandshakes: 1,
	})

	slowConn, err := a.acquireOpening("192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	slowHandshake, err := a.acquireHandshake()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 256; i++ {
		lease, err := a.acquireOpening("198.51.100.1")
		if err != nil {
			t.Fatalf("opening admission blocked by slow handshake at iteration %d: %v", i, err)
		}
		if _, err := a.acquireHandshake(); !errors.Is(err, ErrResourceExhausted) {
			t.Fatalf("handshake admission = %v, want ErrResourceExhausted", err)
		}
		lease.release()
	}

	slowHandshake.release()
	slowConn.release()

	handshake, err := a.acquireHandshake()
	if err != nil {
		t.Fatalf("handshake capacity was not reusable: %v", err)
	}
	handshake.release()

	if got := a.snapshot(); got.OpeningConnections != 0 || got.ActiveConnections != 0 || got.ActiveHandshakes != 0 {
		t.Fatalf("admission state leaked after slow-handshake stress: %+v", got)
	}
}

func TestAdmissionStressGlobalQueuedBytesAcrossManyConnections(t *testing.T) {
	const (
		globalLimit = 64
		perSend     = 8
		workers     = 64
	)

	global := newGlobalByteQuota(globalLimit)
	start := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	var attempted atomic.Int64
	var admitted atomic.Int64
	var maxUsed atomic.Int64

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q := newByteQuota(globalLimit)
			q.setParent(global)
			<-start
			err := q.tryAcquire(perSend)
			attempted.Add(1)
			if err != nil {
				if !errors.Is(err, ErrWouldBlock) || !errors.Is(err, ErrResourceExhausted) {
					t.Errorf("tryAcquire = %v", err)
				}
				return
			}
			admitted.Add(1)
			for {
				used := global.current()
				old := maxUsed.Load()
				if used <= old || maxUsed.CompareAndSwap(old, used) {
					break
				}
			}
			<-release
			q.release(perSend)
		}()
	}

	close(start)
	deadline := time.Now().Add(2 * time.Second)
	for attempted.Load() < workers && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if attempted.Load() != workers {
		t.Fatalf("not all workers attempted admission: %d/%d", attempted.Load(), workers)
	}
	close(release)
	wg.Wait()

	if got := maxUsed.Load(); got > globalLimit {
		t.Fatalf("global queued bytes exceeded limit: got %d, limit %d", got, globalLimit)
	}
	if got := admitted.Load(); got == 0 || got > globalLimit/perSend {
		t.Fatalf("unexpected admitted writers: %d", got)
	}
	if got := global.current(); got != 0 {
		t.Fatalf("global queued bytes leaked: %d", got)
	}
}

func TestAdmissionStressShutdownDuringSustainedOverload(t *testing.T) {
	e, err := New(WithLimits(Limits{
		MaxConnections:          4,
		MaxConnectionsPerPeer:   2,
		MaxConcurrentHandshakes: 1,
	}))
	if err != nil {
		t.Fatal(err)
	}

	const workers = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			addr := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 10000 + id}
			for {
				if err := e.beginOp(); err != nil {
					return
				}
				lease, err := e.acquireOpening(addr)
				if err == nil {
					if handshake, handshakeErr := e.acquireHandshake(); handshakeErr == nil {
						runtime.Gosched()
						handshake.release()
					}
					lease.release()
				}
				e.endOp()
				runtime.Gosched()
			}
		}(i)
	}

	close(start)
	time.Sleep(10 * time.Millisecond)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	select {
	case <-e.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("Engine.Done did not close after overload shutdown: %+v", e.admissionSnapshot())
	}
	if got := e.admissionSnapshot(); got.OpeningConnections != 0 || got.ActiveConnections != 0 || got.ActiveHandshakes != 0 || got.PendingUpgrades != 0 || got.GlobalQueuedBytes != 0 {
		t.Fatalf("admission state leaked after overload shutdown: %+v", got)
	}
}

func BenchmarkGlobalQueuedByteQuotaParallel(b *testing.B) {
	global := newGlobalByteQuota(1 << 30)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		q := newByteQuota(1 << 20)
		q.setParent(global)
		for pb.Next() {
			if err := q.tryAcquire(64); err != nil {
				b.Fatal(err)
			}
			q.release(64)
		}
	})
}
