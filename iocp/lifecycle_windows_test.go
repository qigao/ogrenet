//go:build windows

package iocp

import (
	"errors"
	"testing"
	"time"
)

func TestCloseWakesBlockedGet(t *testing.T) {
	p, err := Open(0)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := p.Get(-1)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Get returned %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake blocked Get")
	}
}
