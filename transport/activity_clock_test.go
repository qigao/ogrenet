package transport

import (
	"testing"
	"time"
)

func TestActivityClockDisabledNeedsNoClock(t *testing.T) {
	if got := newActivityClock(0, 0); got != nil {
		t.Fatalf("newActivityClock(0, 0) = %#v, want nil", got)
	}
}

func TestActivityClockMaxLifetimeWinsEqualDeadline(t *testing.T) {
	clock := newActivityClock(time.Second, time.Second)
	if clock == nil {
		t.Fatal("clock = nil")
	}
	_, kind := clock.nextDeadline()
	if kind != TimeoutMaxLifetime {
		t.Fatalf("next deadline kind = %v, want TimeoutMaxLifetime", kind)
	}
}

func TestActivityClockTouchInvalidatesOldIdleDeadline(t *testing.T) {
	clock := newActivityClock(80*time.Millisecond, 0)
	if clock == nil {
		t.Fatal("clock = nil")
	}
	closing := make(chan struct{})
	expired := make(chan TimeoutKind, 1)
	done := make(chan struct{})
	go func() {
		clock.run(closing, func(kind TimeoutKind) { expired <- kind })
		close(done)
	}()
	defer func() {
		close(closing)
		<-done
	}()

	time.Sleep(50 * time.Millisecond)
	clock.touch()

	select {
	case kind := <-expired:
		t.Fatalf("expired at stale deadline with %v", kind)
	case <-time.After(50 * time.Millisecond):
	}

	select {
	case kind := <-expired:
		if kind != TimeoutConnectionIdle {
			t.Fatalf("expiration kind = %v, want TimeoutConnectionIdle", kind)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("activity clock did not expire after refreshed idle deadline")
	}
}

func TestActivityClockCloseStopsWatchdog(t *testing.T) {
	clock := newActivityClock(time.Second, time.Second)
	closing := make(chan struct{})
	done := make(chan struct{})
	go func() {
		clock.run(closing, func(TimeoutKind) { t.Error("unexpected timeout") })
		close(done)
	}()
	close(closing)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchdog did not stop after close")
	}
}
