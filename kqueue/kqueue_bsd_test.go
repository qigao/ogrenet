//go:build (darwin && (amd64 || arm64)) || (freebsd && (amd64 || arm64 || riscv64))

package kqueue

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestReadiness(t *testing.T) {
	p, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	fds := []int{0, 0}
	if err := unix.Pipe(fds); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = unix.Close(fds[0])
		_ = unix.Close(fds[1])
	}()

	if err := p.Apply(Change{Ident: uint64(fds[0]), Filter: Read, Flags: Add | Clear}); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(fds[1], []byte("x")); err != nil {
		t.Fatal(err)
	}

	events := make([]Event, 2)
	n, err := p.Wait(events, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("got %d events, want 1", n)
	}
	if events[0].Ident != uint64(fds[0]) || events[0].Filter != Read {
		t.Fatalf("got %+v, want read event for fd %d", events[0], fds[0])
	}
}

func TestWake(t *testing.T) {
	p, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	if err := p.Wake(); err != nil {
		t.Fatal(err)
	}
	n, err := p.Wait(make([]Event, 1), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("got %d user events, want 0", n)
	}
}

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

func TestReservedWakeIdentity(t *testing.T) {
	p, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	err = p.Apply(Change{Ident: wakeIdent, Filter: User, Flags: Add})
	if !errors.Is(err, ErrReservedEvent) {
		t.Fatalf("got %v, want ErrReservedEvent", err)
	}
}
