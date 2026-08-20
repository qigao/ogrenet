//go:build windows

package iocp

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

var reservedKey = ^uintptr(0)

// Completion is one packet dequeued from the completion port.
// If an overlapped I/O operation completed with an error, Get returns both the
// populated Completion and a non-nil error.
type Completion struct {
	Bytes      uint32
	Key        uintptr
	Overlapped *windows.Overlapped
}

// Port owns a Windows I/O completion port.
type Port struct {
	handle  windows.Handle
	closed  atomic.Bool
	waiters atomic.Int32
}

// Open creates a new completion port. concurrency is passed directly to
// CreateIoCompletionPort; zero asks Windows to use the processor count.
func Open(concurrency uint32) (*Port, error) {
	h, err := windows.CreateIoCompletionPort(windows.InvalidHandle, 0, 0, concurrency)
	if err != nil {
		return nil, fmt.Errorf("iocp: create: %w", err)
	}
	return &Port{handle: h}, nil
}

// Handle returns the native completion-port handle. The caller must not close it.
func (p *Port) Handle() windows.Handle {
	return p.handle
}

// Associate binds a file or socket handle to the port with an opaque completion key.
func (p *Port) Associate(handle windows.Handle, key uintptr) error {
	if key == reservedKey {
		return ErrReservedKey
	}
	if p.closed.Load() {
		return ErrClosed
	}

	h, err := windows.CreateIoCompletionPort(handle, p.handle, key, 0)
	if err != nil {
		if p.closed.Load() {
			return ErrClosed
		}
		return fmt.Errorf("iocp: associate handle: %w", err)
	}
	if h != p.handle {
		return fmt.Errorf("iocp: associate returned unexpected completion port handle")
	}
	return nil
}

// Post queues an application-defined completion packet.
func (p *Port) Post(c Completion) error {
	if c.Key == reservedKey {
		return ErrReservedKey
	}
	if p.closed.Load() {
		return ErrClosed
	}
	if err := windows.PostQueuedCompletionStatus(p.handle, c.Bytes, c.Key, c.Overlapped); err != nil {
		if p.closed.Load() {
			return ErrClosed
		}
		return fmt.Errorf("iocp: post completion: %w", err)
	}
	return nil
}

// Get waits for one completion packet.
//
// A negative timeout waits indefinitely, zero polls without blocking, and a
// positive timeout is rounded up to the next millisecond.
func (p *Port) Get(timeout time.Duration) (Completion, error) {
	if p.closed.Load() {
		return Completion{}, ErrClosed
	}
	p.waiters.Add(1)
	defer p.waiters.Add(-1)
	if p.closed.Load() {
		return Completion{}, ErrClosed
	}

	var c Completion
	err := windows.GetQueuedCompletionStatus(
		p.handle,
		&c.Bytes,
		&c.Key,
		&c.Overlapped,
		timeoutMillis(timeout),
	)

	if c.Key == reservedKey && c.Overlapped == nil {
		return Completion{}, ErrClosed
	}
	if err == nil {
		return c, nil
	}
	if errors.Is(err, syscall.Errno(windows.WAIT_TIMEOUT)) {
		return Completion{}, ErrTimeout
	}
	if p.closed.Load() {
		return Completion{}, ErrClosed
	}
	return c, fmt.Errorf("iocp: get completion: %w", err)
}

// Close marks the port closed, wakes currently blocked Get calls, and releases
// the native handle. It is idempotent. Associated file/socket handles remain
// owned by their callers.
func (p *Port) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	for i := int32(0); i < p.waiters.Load(); i++ {
		_ = windows.PostQueuedCompletionStatus(p.handle, 0, reservedKey, nil)
	}
	if err := windows.CloseHandle(p.handle); err != nil {
		return fmt.Errorf("iocp: close: %w", err)
	}
	return nil
}

func timeoutMillis(timeout time.Duration) uint32 {
	if timeout < 0 {
		return windows.INFINITE
	}
	if timeout == 0 {
		return 0
	}
	ms := (timeout + time.Millisecond - 1) / time.Millisecond
	if ms >= time.Duration(math.MaxUint32) {
		return math.MaxUint32 - 1
	}
	return uint32(ms)
}
