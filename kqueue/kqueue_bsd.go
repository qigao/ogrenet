//go:build (darwin && (amd64 || arm64)) || (freebsd && (amd64 || arm64 || riscv64))

package kqueue

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Filter is a native kqueue filter identifier.
type Filter int16

// Flags is the native kqueue flags bitmask.
type Flags uint16

const (
	Read    Filter = unix.EVFILT_READ
	Write   Filter = unix.EVFILT_WRITE
	User    Filter = unix.EVFILT_USER
	Add     Flags  = unix.EV_ADD
	Delete  Flags  = unix.EV_DELETE
	Enable  Flags  = unix.EV_ENABLE
	Disable Flags  = unix.EV_DISABLE
	OneShot Flags  = unix.EV_ONESHOT
	Clear   Flags  = unix.EV_CLEAR
	EOF     Flags  = unix.EV_EOF
	Error   Flags  = unix.EV_ERROR
)

const wakeIdent = uint64(math.MaxUint64)

// Change describes one native kevent change. Ident and Filter form the event identity.
type Change struct {
	Ident  uint64
	Filter Filter
	Flags  Flags
	Fflags uint32
	Data   int64
}

// Event is one event returned by Wait.
type Event struct {
	Ident  uint64
	Filter Filter
	Flags  Flags
	Fflags uint32
	Data   int64
}

// Poller owns a kqueue descriptor and one internal EVFILT_USER event used to wake Wait.
// Apply and Wake may run concurrently with Wait; exactly one Wait may be active at a time.
type Poller struct {
	mu sync.RWMutex

	fd int

	closed  atomic.Bool
	waiting atomic.Bool
	waitBuf []unix.Kevent_t
}

// Open creates a kqueue descriptor, marks it close-on-exec, and registers the internal wake event.
func Open() (*Poller, error) {
	fd, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("kqueue: create: %w", err)
	}
	unix.CloseOnExec(fd)

	p := &Poller{fd: fd}
	wake := unix.Kevent_t{
		Ident:  wakeIdent,
		Filter: unix.EVFILT_USER,
		Flags:  unix.EV_ADD | unix.EV_CLEAR,
	}
	if _, err := unix.Kevent(fd, []unix.Kevent_t{wake}, nil, nil); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("kqueue: register wake event: %w", err)
	}
	return p, nil
}

// Apply submits one native kevent change. The pair (wakeIdent, User) is reserved internally.
func (p *Poller) Apply(c Change) error {
	if c.Ident == wakeIdent && c.Filter == User {
		return ErrReservedEvent
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed.Load() {
		return ErrClosed
	}

	ev := unix.Kevent_t{
		Ident:  c.Ident,
		Filter: int16(c.Filter),
		Flags:  uint16(c.Flags),
		Fflags: c.Fflags,
		Data:   c.Data,
	}
	if _, err := unix.Kevent(p.fd, []unix.Kevent_t{ev}, nil, nil); err != nil {
		return fmt.Errorf("kqueue: apply ident %d filter %d: %w", c.Ident, c.Filter, err)
	}
	return nil
}

// Del deletes an event identity from the kqueue.
func (p *Poller) Del(ident uint64, filter Filter) error {
	return p.Apply(Change{Ident: ident, Filter: filter, Flags: Delete})
}

// Wait fills dst with events. A negative timeout waits indefinitely, zero polls,
// and positive durations use kqueue's timespec precision.
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
		p.waitBuf = make([]unix.Kevent_t, len(dst))
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

		var ts unix.Timespec
		var tsp *unix.Timespec
		if timeout >= 0 {
			remaining := timeout - time.Since(started)
			if remaining < 0 {
				remaining = 0
			}
			ts = unix.NsecToTimespec(remaining.Nanoseconds())
			tsp = &ts
		}

		n, err := unix.Kevent(p.fd, nil, p.waitBuf, tsp)
		out := 0
		if err == nil {
			for i := 0; i < n; i++ {
				ev := p.waitBuf[i]
				if ev.Ident == wakeIdent && ev.Filter == unix.EVFILT_USER {
					continue
				}
				dst[out] = Event{
					Ident:  ev.Ident,
					Filter: Filter(ev.Filter),
					Flags:  Flags(ev.Flags),
					Fflags: ev.Fflags,
					Data:   ev.Data,
				}
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
			return 0, fmt.Errorf("kqueue: wait: %w", err)
		}
		return out, nil
	}
}

// Wake interrupts a blocked Wait. The internal EVFILT_USER event is consumed by the wrapper.
func (p *Poller) Wake() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed.Load() {
		return ErrClosed
	}
	if err := p.triggerWake(); err != nil {
		return fmt.Errorf("kqueue: wake: %w", err)
	}
	return nil
}

func (p *Poller) triggerWake() error {
	ev := unix.Kevent_t{
		Ident:  wakeIdent,
		Filter: unix.EVFILT_USER,
		Fflags: unix.NOTE_TRIGGER,
	}
	_, err := unix.Kevent(p.fd, []unix.Kevent_t{ev}, nil, nil)
	return err
}

// Close wakes Wait, closes the kqueue descriptor, and is idempotent. User file
// descriptors referenced by events remain caller-owned.
func (p *Poller) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	_ = p.triggerWake()

	p.mu.Lock()
	defer p.mu.Unlock()
	err := unix.Close(p.fd)
	p.fd = -1
	if errors.Is(err, unix.EBADF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("kqueue: close: %w", err)
	}
	return nil
}
