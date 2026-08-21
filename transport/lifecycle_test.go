package transport

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestSessionLifecycleRequestOwnership(t *testing.T) {
	l := newSessionLifecycle()
	if !l.request(closeGoalWrite) {
		t.Fatal("first write request did not own transition")
	}
	if l.request(closeGoalWrite) {
		t.Fatal("duplicate write request incorrectly became owner")
	}
	if !l.request(closeGoalFull) {
		t.Fatal("full request did not own goal upgrade")
	}
	if l.request(closeGoalFull) {
		t.Fatal("duplicate full request incorrectly became owner")
	}
}

func TestSessionLifecycleAbortFirstWins(t *testing.T) {
	l := newSessionLifecycle()
	if !l.abort(abortCaller) {
		t.Fatal("first abort did not win")
	}
	if l.abort(abortExplicit) {
		t.Fatal("second abort replaced winner")
	}
	if got := l.reason(); got != abortCaller {
		t.Fatalf("abort reason = %v, want %v", got, abortCaller)
	}
	for name, ch := range map[string]<-chan struct{}{
		"abort": l.aborted(),
		"read":  l.readDone(),
		"write": l.writeDone(),
	} {
		select {
		case <-ch:
		default:
			t.Fatalf("%s channel is open after abort", name)
		}
	}
}

func TestSessionLifecycleChannelsCloseExactlyOnce(t *testing.T) {
	l := newSessionLifecycle()
	l.markReadClosed()
	l.markReadClosed()
	l.markWriteClosed()
	l.markWriteClosed()
	l.markTerminal()
	l.markTerminal()
	for name, ch := range map[string]<-chan struct{}{
		"read":     l.readDone(),
		"write":    l.writeDone(),
		"terminal": l.terminalDone(),
	} {
		select {
		case <-ch:
		default:
			t.Fatalf("%s channel is open", name)
		}
	}
}

func TestSessionLifecycleTerminalClosesReadAndWrite(t *testing.T) {
	l := newSessionLifecycle()
	l.markTerminal()
	for name, ch := range map[string]<-chan struct{}{
		"read":     l.readDone(),
		"write":    l.writeDone(),
		"terminal": l.terminalDone(),
	} {
		select {
		case <-ch:
		default:
			t.Fatalf("%s channel is open after terminal", name)
		}
	}
}

func TestSessionLifecycleConcurrentFullRequestHasOneOwner(t *testing.T) {
	l := newSessionLifecycle()
	start := make(chan struct{})
	var owners atomic.Int32
	var wg sync.WaitGroup
	wg.Add(100)
	for range 100 {
		go func() {
			defer wg.Done()
			<-start
			if l.request(closeGoalFull) {
				owners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := owners.Load(); got != 1 {
		t.Fatalf("owners = %d, want 1", got)
	}
}
