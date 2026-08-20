//go:build linux

package epoll

import (
	"errors"
	"testing"
	"time"
)

func TestCloseWakesBlockedWait(t *testing.T) {
	p, err := Open()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := p.Wait(make([]Event, 1), -1)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Wait returned %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake blocked Wait")
	}
}
