//go:build windows

package iocp

import (
	"errors"
	"runtime"
	"testing"
	"time"
)

func TestCloseWakesAllBlockedGet(t *testing.T) {
	p, err := Open(0)
	if err != nil {
		t.Fatal(err)
	}

	const waiterCount = 16
	done := make(chan error, waiterCount)
	for i := 0; i < waiterCount; i++ {
		go func() {
			_, err := p.Get(-1)
			done <- err
		}()
	}

	deadline := time.Now().Add(time.Second)
	for p.waiters.Load() != waiterCount {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d Get calls reached the port", p.waiters.Load(), waiterCount)
		}
		runtime.Gosched()
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake all blocked Get calls")
	}

	for i := 0; i < waiterCount; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("Get returned %v, want ErrClosed", err)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked Get did not return after Close")
		}
	}
}
