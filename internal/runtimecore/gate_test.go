package runtimecore

import "testing"

func TestSendGateCloseWaitsForActiveOwners(t *testing.T) {
	g := NewSendGate()
	if !g.Enter() || !g.Enter() {
		t.Fatal("failed to enter open gate")
	}
	done := g.Close()
	select {
	case <-done:
		t.Fatal("gate drained before active owners left")
	default:
	}

	g.Leave()
	select {
	case <-done:
		t.Fatal("gate drained with one active owner remaining")
	default:
	}

	g.Leave()
	select {
	case <-done:
	default:
		t.Fatal("gate did not drain after final owner left")
	}
}

func TestSendGateRejectsEnterAfterClose(t *testing.T) {
	g := NewSendGate()
	<-g.Close()
	if g.Enter() {
		t.Fatal("enter succeeded after close")
	}
}

func TestSendGateCloseIsIdempotent(t *testing.T) {
	g := NewSendGate()
	first := g.Close()
	second := g.Close()
	if first != second {
		t.Fatal("close returned different drain channels")
	}
	select {
	case <-g.Done():
	default:
		t.Fatal("done channel not closed")
	}
}
