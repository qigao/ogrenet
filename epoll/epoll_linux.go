//go:build linux

package epoll

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
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
// Add, Mod, Del and Wake may be called concurrently with Wait. A Poller permits
// one Wait call at a time; concurrent Wait calls return ErrConcurrentWait.
type Poller struct {
	fd     int
	wakeFD int

	closed atomic.Bool
	waiting atomic.Bool
	waitBuf []unix.EpollEvent
}

// Open creates an epoll instance with close-on-exec enabled.
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

// Add registers fd with the supplied epoll event mask and opaque data value.
func (p *Poller) Add(fd int, events Events, data uint64) error {
	return p.ctl(unix.EPOLL_CTL_ADD, fd, events, data)
}

// Mod changes the event mask and opaque data value associated with fd.
func (p *Poller) Mod(fd int, events Events, data uint64) error {
	return p.ctl(unix.EPOLL_CTL_MOD, fd, events, data)
}

// Del removes fd from the epoll set. The caller still owns fd.
func (p *Poller) Del(fd int) error {
	if p.closed.Load() {
		return ErrClosed
	}
	if err := unix.EpollCtl(p.fd, unix.EPOLL_CTL_DEL, fd, nil); err != nil {
		if p.closed.Load() && errors.Is(err, unix.EBADF) {
			return ErrClosed
		}
		return fmt.Errorf("epoll: delete fd %d: %w", fd, err)
	}
	return nil
}

func (p *Poller) ctl(op, fd int, events Events, data uint64) error {
	if data == wakeData {
		return ErrReservedData
	}
	if p.closed.Load() {
		return ErrClosed
	}

	ev := unix.EpollEvent{Events: uint32(events)}
	putData(&ev, data)
	if err := unix.EpollCtl(p.fd, op, fd, &ev); err != nil {
		if p.closed.Load() && errors.Is(err, unix.EBADF) {
			return ErrClosed
		}
		return fmt.Errorf("epoll: control fd %d: %w", fd, err)
	}
	return nil
}

// Wait fills dst with ready events and returns the number of entries written.
//
// A negative timeout waits indefinitely, zero polls without blocking, and a
// positive timeout is rounded up to the next millisecond because epoll_wait
// accepts millisecond precision.
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
		ms := timeoutMillis(timeout, time.Since(started))
		n, err := unix.EpollWait(p.fd, p.waitBuf, ms)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				if timeout >= 0 && time.Since(started) >= timeout {
					return 0, nil
				}
				continue
			}
			if p.closed.Load() && errors.Is(err, unix.EBADF) {
				return 0, ErrClosed
			}
			return 0, fmt.Errorf("epoll: wait: %w", err)
		}

		out := 0
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

		if p.closed.Load() {
			return 0, ErrClosed
		}
		return out, nil
	}
}

// Wake interrupts a blocked Wait. Wake notifications are consumed internally
// and are not returned as user events.
func (p *Poller) Wake() error {
	if p.closed.Load() {
		return ErrClosed
	}
	if err := p.signalWake(); err != nil {
		if p.closed.Load() && (errors.Is(err, unix.EBADF) || errors.Is(err, unix.EINVAL)) {
			return ErrClosed
		}
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
		if err == nil || errors.Is(err, unix.EINTR) {
			if err == nil {
				return
			}
			continue
		}
		return
	}
}

// Close releases the epoll instance and its internal wake descriptor. It is
// idempotent. File descriptors registered by the caller are never closed.
func (p *Poller) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	_ = p.signalWake()
	epollErr := unix.Close(p.fd)
	wakeErr := unix.Close(p.wakeFD)
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
