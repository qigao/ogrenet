//go:build windows

package iocp

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

var reservedKey = ^uintptr(0)

// Completion is one packet dequeued from the completion port. If an overlapped
// operation failed, Get returns both the populated Completion and a non-nil error.
type Completion struct {
	Bytes      uint32
	Key        uintptr
	Overlapped *windows.Overlapped
}

// Port owns a Windows I/O completion port. Multiple Get calls may block
// concurrently. Close coordinates with all native-handle operations before the
// port handle is released.
type Port struct {
	mu sync.RWMutex

	handle windows.Handle
	closed atomic.Bool
	waiters atomic.Int32
}

// Open creates a completion port. A concurrency value of zero lets Windows use
// the processor count.
func Open(concurrency uint32) (*Port, error) {
	h, err := windows.CreateIoCompletionPort(windows.InvalidHandle, 0, 0, concurrency)
	if err != nil {
		return nil, fmt.Errorf("iocp: create: %w", err)
	}
	return &Port{handle: h}, nil
}

// Associate binds a file or socket handle to the port with an opaque completion key.
func (p *Port) Associate(handle windows.Handle, key uintptr) error {
	if key == reservedKey {
		return ErrReservedKey
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed.Load() {
		return ErrClosed
	}

	h, err := windows.CreateIoCompletionPort(handle, p.handle, key, 0)
	if err != nil {
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
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed.Load() {
		return ErrClosed
	}
	if err := windows.PostQueuedCompletionStatus(p.handle, c.Bytes, c.Key, c.Overlapped); err != nil {
		return fmt.Errorf("iocp: post completion: %w", err)
	}
	return nil
}

// Get waits for one completion packet. A negative timeout waits indefinitely,
// zero polls, and a positive timeout is rounded up to milliseconds.
func (p *Port) Get(timeout time.Duration) (Completion, error) {
	if p.closed.Load() {
		return Completion{}, ErrClosed
	}

	p.waiters.Add(1)
	defer p.waiters.Add(-1)

	p.mu.RLock()
	defer p.mu.RUnlock()
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

// Close wakes blocked Get calls, releases the completion-port handle, and is
// idempotent. Associated file and socket handles remain caller-owned.
func (p *Port) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Wake every Get that had entered the method before closed was published.
	// The handle remains open until all readers leave mu.
	for i := int32(0); i < p.waiters.Load(); i++ {
		_ = windows.PostQueuedCompletionStatus(p.handle, 0, reservedKey, nil)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := windows.CloseHandle(p.handle); err != nil {
		return fmt.Errorf("iocp: close: %w", err)
	}
	p.handle = 0
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
