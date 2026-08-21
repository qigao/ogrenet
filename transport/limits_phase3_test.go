package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestListenerLimitValidationAndStableKind(t *testing.T) {
	if _, err := New(WithLimits(Limits{MaxConnectionsPerListener: -1})); !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("negative listener limit = %v, want ErrInvalidLimits", err)
	}
	if LimitConnectionsPerListener != 0x03 || LimitHandshakes != 0x04 || LimitUpgrades != 0x05 || LimitQueuedBytes != 0x06 {
		t.Fatalf("limit kind IDs changed: listener=%#x handshake=%#x upgrade=%#x queued=%#x", LimitConnectionsPerListener, LimitHandshakes, LimitUpgrades, LimitQueuedBytes)
	}
}

func TestListenerCapacityCountsOpeningAndActive(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 8, MaxConnectionsPerListener: 1})
	capacity := newListenerCapacity(1)
	first, err := a.acquireOpeningWithListener("192.0.2.1", capacity)
	if err != nil {
		t.Fatal(err)
	}
	if got := capacity.current(); got != 1 {
		t.Fatalf("listener used = %d, want 1", got)
	}
	if _, err := a.acquireOpeningWithListener("192.0.2.2", capacity); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("second listener admission = %v, want ErrResourceExhausted", err)
	} else {
		var limitErr *LimitError
		if !errors.As(err, &limitErr) || limitErr.Kind != LimitConnectionsPerListener {
			t.Fatalf("second listener error = %#v", err)
		}
	}
	if !first.activate() {
		t.Fatal("activate failed")
	}
	if got := capacity.current(); got != 1 {
		t.Fatalf("listener used after activate = %d, want 1", got)
	}
	first.release()
	if got := capacity.current(); got != 0 {
		t.Fatalf("listener used after release = %d, want 0", got)
	}
	if got := a.snapshot().RejectedListeners; got != 1 {
		t.Fatalf("listener rejections = %d, want 1", got)
	}
}

func TestListenerCapacityRollsBackWhenGlobalAdmissionFails(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 1, MaxConnectionsPerListener: 1})
	first, err := a.acquireOpening("192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	capacity := newListenerCapacity(1)
	if _, err := a.acquireOpeningWithListener("192.0.2.2", capacity); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("admission = %v, want ErrResourceExhausted", err)
	}
	if got := capacity.current(); got != 0 {
		t.Fatalf("listener capacity leaked after global rejection: %d", got)
	}
	first.release()
}

func TestHTTPConnTrackerCloseRebindAndTransfer(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 1})
	tracker := newHTTPConnTracker()
	raw1, peer1 := net.Pipe()
	defer raw1.Close()
	defer peer1.Close()
	lease, err := a.acquireOpening("192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	holder := tracker.register(raw1, lease)
	ctx := tracker.connContext(context.Background(), raw1)
	if got := httpConnLeaseFromContext(ctx); got != holder {
		t.Fatal("ConnContext did not expose registered holder")
	}
	raw2, peer2 := net.Pipe()
	defer raw2.Close()
	defer peer2.Close()
	if !tracker.rebind(raw1, raw2) {
		t.Fatal("rebind failed")
	}
	if tracker.lookup(raw1) != nil || tracker.lookup(raw2) != holder {
		t.Fatal("rebind did not move holder")
	}
	tracker.connState(raw2, http.StateHijacked)
	transferred := holder.take()
	if transferred == nil || !transferred.activate() {
		t.Fatal("holder transfer failed")
	}
	tracker.connState(raw2, http.StateClosed)
	if got := a.snapshot().ActiveConnections; got != 1 {
		t.Fatalf("ConnState closed released transferred lease: active=%d", got)
	}
	transferred.release()
	if got := a.snapshot(); got.OpeningConnections != 0 || got.ActiveConnections != 0 {
		t.Fatalf("admission leaked after transfer cleanup: %+v", got)
	}
}

func TestTCPPerListenerCapacity(t *testing.T) {
	e, err := New(WithLimits(Limits{MaxConnections: 8, MaxConnectionsPerListener: 1}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opens := make(chan ogrenet.Session, 2)
	ln, err := e.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{
		Open: func(s ogrenet.Session) { opens <- s },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	internal := ln.(*listener)

	first, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var firstSession ogrenet.Session
	select {
	case firstSession = <-opens:
	case <-time.After(2 * time.Second):
		t.Fatal("first TCP session did not open")
	}
	if got := internal.capacity.current(); got != 1 {
		t.Fatalf("listener capacity = %d, want 1", got)
	}

	second, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("second over-limit TCP connection remained open")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("second over-limit TCP connection was not closed promptly")
	}

	_ = first.Close()
	// Peer close is observed as a read-half close. The server Session still owns
	// its write side, so it must keep consuming listener capacity until the
	// Session itself terminates.
	half, ok := firstSession.(halfCloseProbe)
	if !ok {
		t.Fatalf("%T does not expose read-half state", firstSession)
	}
	select {
	case <-half.ReadClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server read half did not close after peer close")
	}
	if got := internal.capacity.current(); got != 1 {
		t.Fatalf("listener capacity released on read half-close: %d", got)
	}
	if err := firstSession.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && internal.capacity.current() != 0 {
		time.Sleep(time.Millisecond)
	}
	if got := internal.capacity.current(); got != 0 {
		t.Fatalf("listener capacity did not return after Session close: %d", got)
	}

	third, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	select {
	case <-opens:
	case <-time.After(2 * time.Second):
		t.Fatal("listener capacity was not reusable")
	}
}

func TestWSPreHeaderConnectionConsumesCapacity(t *testing.T) {
	e, err := New(WithLimits(Limits{MaxConnections: 1, MaxConnectionsPerListener: 1}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := e.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeWS, Host: "127.0.0.1", Port: 0, Path: "/"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	first, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	waitForAdmission(t, e, func(s admissionSnapshot) bool { return s.OpeningConnections == 1 })

	second, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if _, err := second.Read(one); err == nil {
		t.Fatal("over-limit pre-header connection remained open")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("over-limit pre-header connection was not closed promptly")
	}

	_ = first.Close()
	waitForAdmission(t, e, func(s admissionSnapshot) bool { return s.OpeningConnections == 0 })
}

func waitForAdmission(t *testing.T, e *Engine, ready func(admissionSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready(e.admissionSnapshot()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("admission condition not reached: %+v", e.admissionSnapshot())
}

func TestAdmissionFloodLeavesNoTokens(t *testing.T) {
	a := newAdmissionController(Limits{MaxConnections: 32, MaxConnectionsPerListener: 8})
	capacity := newListenerCapacity(8)
	var wg sync.WaitGroup
	var maxSeen atomic.Int64
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := a.acquireOpeningWithListener("192.0.2.1", capacity)
			if err != nil {
				return
			}
			for {
				n := capacity.current()
				old := maxSeen.Load()
				if n <= old || maxSeen.CompareAndSwap(old, n) {
					break
				}
			}
			lease.activate()
			runtime.Gosched()
			lease.release()
		}()
	}
	wg.Wait()
	if got := maxSeen.Load(); got > 8 {
		t.Fatalf("listener capacity exceeded: %d", got)
	}
	if got := capacity.current(); got != 0 {
		t.Fatalf("listener capacity leaked: %d", got)
	}
	if got := a.snapshot(); got.OpeningConnections != 0 || got.ActiveConnections != 0 {
		t.Fatalf("admission tokens leaked: %+v", got)
	}
}

func BenchmarkAdmissionAcquireRelease(b *testing.B) {
	a := newAdmissionController(Limits{MaxConnections: 1 << 20})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lease, err := a.acquireOpening("")
		if err != nil {
			b.Fatal(err)
		}
		lease.activate()
		lease.release()
	}
}

func BenchmarkAdmissionAcquireReleaseParallel(b *testing.B) {
	a := newAdmissionController(Limits{MaxConnections: 1 << 20})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lease, err := a.acquireOpening("")
			if err != nil {
				b.Fatal(err)
			}
			lease.activate()
			lease.release()
		}
	})
}
