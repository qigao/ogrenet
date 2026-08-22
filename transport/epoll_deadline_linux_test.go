//go:build linux

package transport

import (
	"testing"
	"time"
)

type testDeadlineResource struct {
	*testEventResource
	gens  [6]uint64
	fired chan epollDeadlineKind
}

func newTestDeadlineResource(id uint64) *testDeadlineResource {
	return &testDeadlineResource{
		testEventResource: newTestEventResource(id, -1, 1),
		fired:             make(chan epollDeadlineKind, 8),
	}
}

func (x *testDeadlineResource) currentDeadlineGeneration(kind epollDeadlineKind) uint64 {
	return x.gens[kind]
}

func (x *testDeadlineResource) onReactorDeadline(_ *epollReactor, kind epollDeadlineKind, _ uint64) {
	x.fired <- kind
}

func TestEpollDeadlineHeapOrdersEarliest(t *testing.T) {
	r := newTestEpollReactor(t)
	connect := newTestDeadlineResource(1)
	write := newTestDeadlineResource(2)
	connect.gens[epollDeadlineConnect] = 1
	write.gens[epollDeadlineWrite] = 1
	if err := r.registerResource(connect); err != nil {
		t.Fatal(err)
	}
	if err := r.registerResource(write); err != nil {
		t.Fatal(err)
	}

	base := time.Unix(100, 0)
	r.scheduleDeadline(2, epollDeadlineWrite, 1, base.Add(2*time.Second))
	r.scheduleDeadline(1, epollDeadlineConnect, 1, base.Add(time.Second))
	if got := r.nextWaitTimeout(base); got != time.Second {
		t.Fatalf("timeout=%v, want %v", got, time.Second)
	}

	r.runExpiredDeadlines(base.Add(time.Second))
	select {
	case got := <-connect.fired:
		if got != epollDeadlineConnect {
			t.Fatalf("kind=%v, want connect", got)
		}
	default:
		t.Fatal("earliest deadline did not fire")
	}
	select {
	case got := <-write.fired:
		t.Fatalf("later deadline fired early: %v", got)
	default:
	}
	if got := r.nextWaitTimeout(base.Add(time.Second)); got != time.Second {
		t.Fatalf("next timeout=%v, want %v", got, time.Second)
	}
}

func TestEpollDeadlineIgnoresStaleGeneration(t *testing.T) {
	r := newTestEpollReactor(t)
	res := newTestDeadlineResource(3)
	res.gens[epollDeadlineWrite] = 2
	if err := r.registerResource(res); err != nil {
		t.Fatal(err)
	}

	base := time.Unix(200, 0)
	r.scheduleDeadline(3, epollDeadlineWrite, 1, base)
	r.runExpiredDeadlines(base)
	select {
	case got := <-res.fired:
		t.Fatalf("stale deadline fired: %v", got)
	default:
	}
	if got := r.nextWaitTimeout(base); got >= 0 {
		t.Fatalf("stale heap entry remained live: timeout=%v", got)
	}
}

func TestEpollDeadlineWaitTimeoutZeroWhenExpired(t *testing.T) {
	r := newTestEpollReactor(t)
	res := newTestDeadlineResource(4)
	res.gens[epollDeadlineReadIdle] = 1
	if err := r.registerResource(res); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(300, 0)
	r.scheduleDeadline(4, epollDeadlineReadIdle, 1, base)
	if got := r.nextWaitTimeout(base); got != 0 {
		t.Fatalf("expired timeout=%v, want 0", got)
	}
}

func TestEpollDeadlineWaitTimeoutNegativeWhenEmpty(t *testing.T) {
	r := newTestEpollReactor(t)
	if got := r.nextWaitTimeout(time.Unix(400, 0)); got >= 0 {
		t.Fatalf("empty timeout=%v, want negative", got)
	}
}
