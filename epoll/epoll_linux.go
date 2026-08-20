//go:build linux

package epoll

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Events is the native epoll event bitmask.
type Events uint32

const (
	Readable      Events = unix.EPOLLIN
	Writable      Events = unix.EPOLLOUT
	Priority      Events = unix.EPOLLPRI
	Hangup        Events = unix.EPOLLHUP
	Error         Events = unix.EPOLLERR
	PeerClosed    Events = unix.EPOLLRDHUP
	EdgeTriggered Events = 1 << 31
	OneShot       Events = 1 << 30
	Exclusive     Events = 1 << 28
)

const wakeData = math.MaxUint64

// Event is one readiness notification returned by Wait.
// Data is the opaque 64-bit value supplied to Add or Mod.
type Event struct {
	Events Events
	Data   uint64
}

// Poller owns an epoll instance and an eventfd used to interrupt Wait.
//
// Add, Mod, Del and Wake may run concurrently with Wait. Exactly one Wait may
// be active at a time. Close safely coordinates with all operations so native
// descriptors cannot be reused while a method is still issuing a syscall.
type Poller struct {
	mu sync.RWMutex

	fd     int
	wakeFD int

	closed  atomic.Bool
	waiting atomic.Bool
	waitBuf []unix.EpollEvent
}

// Open creates an epoll instance and its internal wake eventfd with close-on-exec enabled.
func Open() (*Poller, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll: create: %w", err)
	}

	wakeFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("epoll: create wake eventfd: %w", err)
	}

	p := &Poller{fd: fd, wakeFD: wakeFD}
	ev := unix.EpollEvent{Events: unix.EPOLLIN}
	putData(&ev, wakeData)
	if err := unix.EpollCtl(fd, unix.EPOLL_CTL_ADD, wakeFD, &ev); err != nil {
		_ = unix.Close(wakeFD)
		_ = unix.Close(fd)
		return nil, fmt.Errorf("epoll: register wake eventfd: %w", err)
	}
	return p, nil
}

// Add registers fd with an event mask and opaque data value.
func (p *Poller) Add(fd int, events Events, data uint64) error {
	return p.ctl(unix.EPOLL_CTL_ADD, fd, events, data)
}

// Mod changes the event mask and opaque data value associated with fd.
func (p *Poller) Mod(fd int, events Events, data uint64) error {
	return p.ctl(unix.EPOLL_CTL_MOD, fd, events, data)
}

// Del removes fd from the epoll set. The caller retains ownership of fd.
func (p *Poller) Del(fd int) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed.Load() {
		return ErrClosed
	}
	if err := unix.EpollCtl(p.fd, unix.EPOLL_CTL_DEL, fd, nil); err != nil {
		return fmt.Errorf("epoll: delete fd %d: %w", fd, err)
	}
	return nil
}

func (p *Poller) ctl(op, fd int, events Events, data uint64) error {
	if data == wakeData {
		return ErrReservedData
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed.Load() {
		return ErrClosed
	}

	ev := unix.EpollEvent{Events: uint32(events)}
	putData(&ev, data)
	if err := unix.EpollCtl(p.fd, op, fd, &ev); err != nil {
		return fmt.Errorf("epoll: control fd %d: %w", fd, err)
	}
	return nil
}

// Wait fills dst with ready events and returns the number of entries written.
// A negative timeout waits indefinitely, zero polls, and positive durations are
// rounded up to epoll_wait's millisecond precision.
func (p *Poller) Wait(dst []Event, timeout time.Duration) (int, error) {
	if len(dst) == 0 {
		return 0, ErrNoEvents
	}
	if p.closed.Load() {
		return 0, ErrClosed
	}
	if !p.waiting.CompareAndSwap(false, true) {
		return 0, ErrConcurrentWait
	}
	defer p.waiting.Store(false)

	if cap(p.waitBuf) < len(dst) {
		p.waitBuf = make([]unix.EpollEvent, len(dst))
	} else {
		p.waitBuf = p.waitBuf[:len(dst)]
	}

	started := time.Now()
	for {
		p.mu.RLock()
		if p.closed.Load() {
			p.mu.RUnlock()
			return 0, ErrClosed
		}

		ms := timeoutMillis(timeout, time.Since(started))
		n, err := unix.EpollWait(p.fd, p.waitBuf, ms)
		out := 0
		if err == nil {
			for i := 0; i < n; i++ {
				ev := p.waitBuf[i]
				data := eventData(ev)
				if data == wakeData {
					p.drainWake()
					continue
				}
				dst[out] = Event{Events: Events(ev.Events), Data: data}
				out++
			}
		}
		p.mu.RUnlock()

		if p.closed.Load() {
			return 0, ErrClosed
		}
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				if timeout >= 0 && time.Since(started) >= timeout {
					return 0, nil
				}
				continue
			}
			return 0, fmt.Errorf("epoll: wait: %w", err)
		}
		return out, nil
	}
}

// Wake interrupts a blocked Wait. Internal wake notifications are not returned in dst.
func (p *Poller) Wake() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed.Load() {
		return ErrClosed
	}
	if err := p.signalWake(); err != nil {
		return fmt.Errorf("epoll: wake: %w", err)
	}
	return nil
}

func (p *Poller) signalWake() error {
	var b [8]byte
	binary.NativeEndian.PutUint64(b[:], 1)
	for {
		_, err := unix.Write(p.wakeFD, b[:])
		if err == nil || errors.Is(err, unix.EAGAIN) {
			return nil
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return err
	}
}

func (p *Poller) drainWake() {
	var b [8]byte
	for {
		_, err := unix.Read(p.wakeFD, b[:])
		if err == nil {
			return
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return
	}
}

// Close wakes Wait, releases internal descriptors, and is idempotent. Registered
// descriptors remain owned by the caller.
func (p *Poller) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	// The closing goroutine is the only goroutine that can transition closed to
	// true, so internal descriptors are guaranteed to remain open until it takes
	// mu for the actual close. Signaling first prevents a blocked Wait from
	// holding the read side of mu forever.
	_ = p.signalWake()

	p.mu.Lock()
	defer p.mu.Unlock()
	epollErr := unix.Close(p.fd)
	wakeErr := unix.Close(p.wakeFD)
	p.fd = -1
	p.wakeFD = -1
	if errors.Is(epollErr, unix.EBADF) {
		epollErr = nil
	}
	if errors.Is(wakeErr, unix.EBADF) {
		wakeErr = nil
	}
	return errors.Join(epollErr, wakeErr)
}

func putData(ev *unix.EpollEvent, data uint64) {
	ev.Fd = int32(uint32(data))
	ev.Pad = int32(uint32(data >> 32))
}

func eventData(ev unix.EpollEvent) uint64 {
	return uint64(uint32(ev.Fd)) | uint64(uint32(ev.Pad))<<32
}

func timeoutMillis(timeout, elapsed time.Duration) int {
	if timeout < 0 {
		return -1
	}
	remaining := timeout - elapsed
	if remaining <= 0 {
		return 0
	}
	ms := (remaining + time.Millisecond - 1) / time.Millisecond
	if ms > time.Duration(math.MaxInt32) {
		return math.MaxInt32
	}
	return int(ms)
}
