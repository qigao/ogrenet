//go:build linux

package transport

import (
	"net"
	"testing"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
)

type blockingEpollWorkerTask struct {
	entered chan struct{}
	release <-chan struct{}
}

func (x *blockingEpollWorkerTask) runEpollWorkerTask() {
	close(x.entered)
	<-x.release
}

func newCodecTestSession(e *epollEngine, r *epollReactor, id uint64) *epollSession {
	return newEpollBootstrapSession(
		e,
		r,
		id,
		-1,
		ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 1},
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2},
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3},
		nil,
		nil,
		nil,
	)
}

func TestEpollCodecFactoryRunsOffReactor(t *testing.T) {
	r := newTestEpollReactor(t)
	started := make(chan struct{})
	release := make(chan struct{})
	capacity := make(chan struct{}, 1)

	cfg := defaultConfig()
	cfg.framerFactory = func() wire.Framer {
		close(started)
		<-release
		return wire.New(nil)
	}
	e := &epollEngine{cfg: cfg}
	e.callbacks = newEpollCallbackExecutor(1, 1, func() {
		select {
		case capacity <- struct{}{}:
		default:
		}
	})
	defer func() {
		close(release)
		<-capacity
		r.drainInbox()
		e.callbacks.stopIdle()
	}()

	session := newCodecTestSession(e, r, 101)
	r.resources[session.id] = session
	r.requeue(session)
	r.drainRunnable()
	waitTestSignal(t, started, "blocking framer factory")

	independent := newTestEventResource(102, -1, 1)
	r.resources[independent.id] = independent
	r.requeue(independent)
	r.drainRunnable()
	if got := independent.runs.Load(); got != 1 {
		t.Fatalf("independent reactor item runs=%d, want 1", got)
	}
}

func TestEpollCodecWorkerCapacityReleaseCannotBeLost(t *testing.T) {
	r := newTestEpollReactor(t)
	blockingRelease := make(chan struct{})
	blocking := &blockingEpollWorkerTask{entered: make(chan struct{}), release: blockingRelease}
	setupStarted := make(chan struct{})
	capacity := make(chan struct{}, 2)

	cfg := defaultConfig()
	cfg.framerFactory = func() wire.Framer {
		close(setupStarted)
		return wire.New(nil)
	}
	e := &epollEngine{cfg: cfg}
	e.callbacks = newEpollCallbackExecutor(1, 0, func() {
		r.signalControl(epollControlWorkerCapacity)
		capacity <- struct{}{}
	})
	r.workerCapacityAvailable = e.callbacks.hasCapacity
	defer e.callbacks.stopIdle()

	if !e.callbacks.tryReserve() {
		t.Fatal("initial worker reservation failed")
	}
	e.callbacks.submitReserved(blocking)
	waitTestSignal(t, blocking.entered, "blocking worker task")

	session := newCodecTestSession(e, r, 201)
	r.resources[session.id] = session
	r.requeue(session)
	r.drainRunnable()
	if !r.hasWorkerBlocked.Load() {
		t.Fatal("codec setup did not publish worker-blocked state")
	}

	close(blockingRelease)
	waitTestSignal(t, capacity, "worker capacity release")
	r.drainControl()
	r.drainRunnable()
	waitTestSignal(t, setupStarted, "codec setup after capacity release")
	waitTestSignal(t, capacity, "codec setup completion")
	r.drainInbox()
	if !session.codecSetupComplete() {
		t.Fatal("codec setup result was not published")
	}
}
