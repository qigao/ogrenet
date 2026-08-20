//go:build linux

package epoll

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestPollerReadiness(t *testing.T) {
	p, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = unix.Close(fds[0])
		_ = unix.Close(fds[1])
	}()

	const data = uint64(0xfeedbeef01020304)
	if err := p.Add(fds[0], Readable|PeerClosed, data); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(fds[1], []byte("x")); err != nil {
		t.Fatal(err)
	}

	events := make([]Event, 4)
	n, err := p.Wait(events, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("got %d events, want 1", n)
	}
	if events[0].Data != data {
		t.Fatalf("got data %#x, want %#x", events[0].Data, data)
	}
	if events[0].Events&Readable == 0 {
		t.Fatalf("event mask %#x does not contain Readable", events[0].Events)
	}
}

func TestPollerModAndDel(t *testing.T) {
	p, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = unix.Close(fds[0])
		_ = unix.Close(fds[1])
	}()

	if err := p.Add(fds[0], Readable, 1); err != nil {
		t.Fatal(err)
	}
	if err := p.Mod(fds[0], Readable|OneShot, 2); err != nil {
		t.Fatal(err)
	}
	if err := p.Del(fds[0]); err != nil {
		t.Fatal(err)
	}

	if _, err := unix.Write(fds[1], []byte("x")); err != nil {
		t.Fatal(err)
	}
	events := make([]Event, 1)
	n, err := p.Wait(events, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("got %d events after delete, want 0", n)
	}
}

func TestPollerWake(t *testing.T) {
	p, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	if err := p.Wake(); err != nil {
		t.Fatal(err)
	}
	events := make([]Event, 1)
	started := time.Now()
	n, err := p.Wait(events, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("got %d user events, want 0", n)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("Wake did not interrupt Wait promptly")
	}
}

func TestPollerCloseIsIdempotent(t *testing.T) {
	p, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	events := make([]Event, 1)
	if _, err := p.Wait(events, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("got error %v, want ErrClosed", err)
	}
	if err := p.Wake(); !errors.Is(err, ErrClosed) {
		t.Fatalf("got error %v, want ErrClosed", err)
	}
}

func TestReservedData(t *testing.T) {
	p, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	if err := p.Add(0, Readable, wakeData); !errors.Is(err, ErrReservedData) {
		t.Fatalf("got error %v, want ErrReservedData", err)
	}
}

func TestTimeoutMillis(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		elapsed time.Duration
		want    int
	}{
		{name: "infinite", timeout: -1, want: -1},
		{name: "zero", timeout: 0, want: 0},
		{name: "round up", timeout: 1500 * time.Microsecond, want: 2},
		{name: "elapsed", timeout: 10 * time.Millisecond, elapsed: 9*time.Millisecond + 1, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := timeoutMillis(tt.timeout, tt.elapsed); got != tt.want {
				t.Fatalf("timeoutMillis(%v, %v) = %d, want %d", tt.timeout, tt.elapsed, got, tt.want)
			}
		})
	}
}
