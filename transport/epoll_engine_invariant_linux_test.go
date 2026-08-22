//go:build linux

package transport

import "testing"

type epollEngineInvariantSnapshot struct {
	ReactorResources int
	ReactorInbox     int
	ReactorRunnable  int
	WorkerBlocked    int
	CallbackReserved int
	ManagedResources int

	OpeningConnections  uint64
	ActiveConnections   uint64
	DrainingConnections uint64
	GlobalQueuedBytes    uint64
}

func snapshotEpollEngineInvariants(e *epollEngine) epollEngineInvariantSnapshot {
	var out epollEngineInvariantSnapshot
	if e == nil {
		return out
	}
	for _, r := range e.reactors {
		if r == nil {
			continue
		}
		out.ReactorResources += len(r.resources)
		r.inboxMu.Lock()
		if r.inboxHead != nil {
			out.ReactorInbox++
		}
		r.inboxMu.Unlock()
		if r.runnableHead < len(r.runnable) {
			out.ReactorRunnable += len(r.runnable) - r.runnableHead
		}
		out.WorkerBlocked += len(r.workerBlocked)
	}
	if e.callbacks != nil {
		out.CallbackReserved = e.callbacks.reservedCount()
	}
	e.mu.Lock()
	out.ManagedResources = len(e.managed)
	e.mu.Unlock()
	stats := e.Stats()
	out.OpeningConnections = stats.OpeningConnections
	out.ActiveConnections = stats.ActiveConnections
	out.DrainingConnections = stats.DrainingConnections
	out.GlobalQueuedBytes = stats.GlobalQueuedBytes
	return out
}

func assertEpollEngineZeroInvariants(t *testing.T, e *epollEngine) {
	t.Helper()
	got := snapshotEpollEngineInvariants(e)
	want := epollEngineInvariantSnapshot{}
	if got != want {
		t.Fatalf("epoll Engine invariant snapshot after Done = %+v, want %+v", got, want)
	}
}

func TestEpollEngineDoneHasZeroInvariantSnapshot(t *testing.T) {
	e, _, _, _ := newEpollEngineShutdownPeer(t, nil)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	waitEpollEngineSignal(t, e.Done(), "Engine.Done for invariant snapshot")
	assertEpollEngineZeroInvariants(t, e)
}
