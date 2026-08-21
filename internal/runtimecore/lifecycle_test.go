package runtimecore

import (
	"sync/atomic"
	"testing"
)

func TestLifecycleWriteThenFullEscalation(t *testing.T) {
	l := NewLifecycle()
	owner, previous := l.RequestWithPrevious(GoalWrite)
	if !owner || previous != GoalRunning {
		t.Fatalf("write request owner=%v previous=%v", owner, previous)
	}
	select {
	case <-l.WriteRequested():
	default:
		t.Fatal("write request signal not closed")
	}
	select {
	case <-l.FullRequested():
		t.Fatal("full request closed before escalation")
	default:
	}

	owner, previous = l.RequestWithPrevious(GoalFull)
	if !owner || previous != GoalWrite {
		t.Fatalf("full request owner=%v previous=%v", owner, previous)
	}
	select {
	case <-l.FullRequested():
	default:
		t.Fatal("full request signal not closed")
	}
}

func TestLifecycleAbortPublishesBeforeSignals(t *testing.T) {
	l := NewLifecycle()
	var published atomic.Bool
	observed := make(chan bool, 1)
	go func() {
		<-l.Aborted()
		observed <- published.Load()
	}()

	if !l.AbortWith(AbortFailure, func() { published.Store(true) }) {
		t.Fatal("first abort did not own transition")
	}
	if !<-observed {
		t.Fatal("abort became observable before publication")
	}
}

func TestLifecycleFirstAbortOwnerWins(t *testing.T) {
	l := NewLifecycle()
	if !l.Abort(AbortFailure) {
		t.Fatal("first abort did not win")
	}
	if l.Abort(AbortExplicit) {
		t.Fatal("second abort replaced first owner")
	}
	if got := l.Reason(); got != AbortFailure {
		t.Fatalf("reason=%v", got)
	}
}

func TestLifecycleTerminalClosesReadWriteAndTerminal(t *testing.T) {
	l := NewLifecycle()
	l.MarkTerminal()
	for name, ch := range map[string]<-chan struct{}{
		"read":     l.ReadDone(),
		"write":    l.WriteDone(),
		"terminal": l.TerminalDone(),
	} {
		select {
		case <-ch:
		default:
			t.Fatalf("%s completion signal not closed", name)
		}
	}
}

func TestLifecycleAbortCannotBeReplacedByTerminalMark(t *testing.T) {
	l := NewLifecycle()
	if !l.Abort(AbortCaller) {
		t.Fatal("abort did not win")
	}
	if l.TryMarkTerminal() {
		t.Fatal("terminal mark replaced abort ownership")
	}
	if got := l.Reason(); got != AbortCaller {
		t.Fatalf("reason=%v", got)
	}
	l.MarkTerminal()
	if got := l.Reason(); got != AbortCaller {
		t.Fatalf("reason changed after terminal mark: %v", got)
	}
}
