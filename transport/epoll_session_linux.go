//go:build linux

package transport

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
	"github.com/qigao/ogrenet/wire"
	"golang.org/x/sys/unix"
)

type epollSessionState uint8

const (
	epollSessionHandoff epollSessionState = iota + 1
	epollSessionConnecting
	epollSessionCodecSetup
	epollSessionOpening
	epollSessionActive
	epollSessionTerminal
	epollSessionClosed
)

type epollSession struct {
	engine  *epollEngine
	reactor *epollReactor
	node    epollInboxNode

	id         uint64
	fd         int
	state      epollSessionState
	registered bool
	endpoint   ogrenet.Endpoint
	local      *net.TCPAddr
	remote     *net.TCPAddr
	handler    ogrenet.Handler
	lease      *connectionLease
	parent     *epollListener
	dial       *epollDialState

	framer     wire.Framer
	wireFramer bool

	setupMu        sync.Mutex
	setupDone      bool
	setupSubmitted bool
	setupErr       error
	setupFramer    wire.Framer

	engineAbort       atomic.Uint32
	shutdownRequested atomic.Bool
	done              chan struct{}
	doneOnce          sync.Once
}

func newEpollBootstrapSession(
	engine *epollEngine,
	reactor *epollReactor,
	id uint64,
	fd int,
	endpoint ogrenet.Endpoint,
	local, remote *net.TCPAddr,
	handler ogrenet.Handler,
	lease *connectionLease,
	parent *epollListener,
) *epollSession {
	s := &epollSession{
		engine:   engine,
		reactor:  reactor,
		id:       id,
		fd:       fd,
		state:    epollSessionCodecSetup,
		endpoint: endpoint,
		local:    cloneTCPAddr(local),
		remote:   cloneTCPAddr(remote),
		handler:  handler,
		lease:    lease,
		parent:   parent,
		done:     make(chan struct{}),
	}
	s.node.owner = s
	return s
}

func cloneTCPAddr(addr *net.TCPAddr) *net.TCPAddr {
	if addr == nil {
		return nil
	}
	out := *addr
	out.IP = append(net.IP(nil), addr.IP...)
	return &out
}

func (s *epollSession) inboxNode() *epollInboxNode { return &s.node }
func (s *epollSession) resourceID() uint64         { return s.id }
func (s *epollSession) resourceFD() int            { return s.fd }
func (s *epollSession) managedID() uint64          { return s.id }
func (s *epollSession) managedKind() epollManagedKind {
	return epollManagedSession
}

func (s *epollSession) prepareEngineDrain() {
	if s != nil && s.lease != nil {
		s.lease.beginDrain()
	}
}

func (s *epollSession) requestEngineShutdown() {
	if s == nil {
		return
	}
	s.shutdownRequested.Store(true)
	if s.reactor != nil {
		s.reactor.signal(s)
	}
}

func (s *epollSession) requestEngineAbort(reason abortReason) {
	if s == nil || reason == abortNone {
		return
	}
	for {
		current := s.engineAbort.Load()
		if current != uint32(abortNone) || s.engineAbort.CompareAndSwap(current, uint32(reason)) {
			break
		}
	}
	if s.reactor != nil {
		s.reactor.signal(s)
	}
}

func (s *epollSession) onReactorInbox(r *epollReactor) {
	if s == nil || r == nil || s.state == epollSessionClosed {
		return
	}
	if s.engineAbort.Load() != uint32(abortNone) {
		if s.dial != nil {
			s.dial.finish(nil, ErrClosed)
		}
		s.state = epollSessionTerminal
	}
	if s.shutdownRequested.Load() && s.state < epollSessionActive {
		if s.dial != nil {
			s.dial.finish(nil, ErrClosed)
		}
		s.state = epollSessionTerminal
	}

	switch s.state {
	case epollSessionHandoff:
		s.adoptAcceptedFD(r)
	case epollSessionConnecting:
		s.driveNativeConnect(r)
	case epollSessionCodecSetup:
		if s.codecSetupComplete() {
			s.applyCodecSetup(r)
		}
	case epollSessionTerminal:
		s.finalizeReactor(r)
	}
}

func (s *epollSession) onReactorEvent(r *epollReactor, events epoll.Events) {
	if s == nil || r == nil {
		return
	}
	if s.state == epollSessionConnecting {
		s.onNativeConnectEvent(r, events)
	}
}

func (s *epollSession) onReactorRunnable(r *epollReactor) {
	if s == nil || r == nil {
		return
	}
	switch s.state {
	case epollSessionConnecting:
		s.driveNativeConnect(r)
	case epollSessionCodecSetup:
		s.startCodecSetup(r)
	case epollSessionTerminal:
		s.finalizeReactor(r)
	}
}

func (s *epollSession) adoptAcceptedFD(r *epollReactor) {
	if s.fd < 0 {
		s.state = epollSessionTerminal
		s.finalizeReactor(r)
		return
	}
	if err := r.registerResource(s); err != nil {
		s.state = epollSessionTerminal
		s.finalizeReactor(r)
		return
	}
	if err := r.addFD(s.fd, epoll.Readable|epoll.PeerClosed|epoll.Error|epoll.EdgeTriggered, s.id); err != nil {
		delete(r.resources, s.id)
		s.state = epollSessionTerminal
		s.finalizeReactor(r)
		return
	}
	// Poller.Add is the accepted-fd ownership transfer point. From here on the
	// target reactor is the only goroutine allowed to issue syscalls on this fd.
	s.registered = true
	s.state = epollSessionCodecSetup
	r.requeue(s)
}

func (s *epollSession) startCodecSetup(r *epollReactor) {
	s.setupMu.Lock()
	if s.setupDone || s.setupSubmitted {
		s.setupMu.Unlock()
		return
	}
	s.setupMu.Unlock()

	if s.engine == nil || s.engine.callbacks == nil || !s.engine.callbacks.tryReserve() {
		r.blockOnWorker(s)
		return
	}

	s.setupMu.Lock()
	if s.setupDone || s.setupSubmitted {
		s.setupMu.Unlock()
		s.engine.callbacks.releaseReserved()
		return
	}
	s.setupSubmitted = true
	s.setupMu.Unlock()
	s.engine.callbacks.submitReserved(&epollCodecSetupTask{session: s})
}

func (s *epollSession) storeCodecSetup(framer wire.Framer, err error) {
	s.setupMu.Lock()
	if s.setupDone {
		s.setupMu.Unlock()
		return
	}
	s.setupFramer = framer
	s.setupErr = err
	s.setupDone = true
	s.setupMu.Unlock()
}

func (s *epollSession) notifyCodecSetup() {
	s.setupMu.Lock()
	s.setupSubmitted = false
	done := s.setupDone
	s.setupMu.Unlock()
	if done && s.reactor != nil {
		s.reactor.signal(s)
	}
}

func (s *epollSession) codecSetupComplete() bool {
	if s == nil {
		return false
	}
	s.setupMu.Lock()
	defer s.setupMu.Unlock()
	return s.setupDone && !s.setupSubmitted
}

func (s *epollSession) codecSetupOutstanding() bool {
	if s == nil {
		return false
	}
	s.setupMu.Lock()
	defer s.setupMu.Unlock()
	return s.setupSubmitted
}

func (s *epollSession) applyCodecSetup(r *epollReactor) {
	s.setupMu.Lock()
	framer := s.setupFramer
	err := s.setupErr
	s.setupMu.Unlock()
	if err == nil && framer == nil {
		err = ErrNilFramer
	}
	if err != nil {
		if s.dial != nil {
			s.engine.emitNativeConnect(0, s.endpoint, cloneTCPAddr(s.local), cloneTCPAddr(s.remote), s.dial.observerStart, nil)
			s.dial.finish(nil, err)
		}
		s.state = epollSessionTerminal
		s.finalizeReactor(r)
		return
	}
	if s.lease != nil && !s.lease.activate() {
		if s.dial != nil {
			opErr := classifyOperational(OpDial, ogrenet.SchemeTCP, cloneTCPAddr(s.local), cloneTCPAddr(s.remote), ErrClosed, hintNone)
			s.engine.emitNativeConnect(0, s.endpoint, cloneTCPAddr(s.local), cloneTCPAddr(s.remote), s.dial.observerStart, nil)
			s.dial.finish(nil, opErr)
		}
		s.state = epollSessionTerminal
		s.finalizeReactor(r)
		return
	}

	s.framer = framer
	_, s.wireFramer = framer.(*wire.Codec)
	if s.parent != nil {
		if s.parent.stats != nil {
			s.parent.stats.accepted.Add(1)
		}
		if s.engine != nil && s.engine.observer != nil {
			s.engine.observer.emit(ogrenet.Event{
				Kind:       ogrenet.EventAccept,
				Resource:   ogrenet.ResourceSession,
				ResourceID: s.id,
				ParentID:   s.parent.id,
				Protocol:   ogrenet.SchemeTCP,
				Local:      cloneTCPAddr(s.local),
				Remote:     cloneTCPAddr(s.remote),
			})
		}
	} else if s.dial != nil {
		s.engine.emitNativeConnect(s.id, s.endpoint, cloneTCPAddr(s.local), cloneTCPAddr(s.remote), s.dial.observerStart, nil)
		s.dial.finish(s, nil)
	}
	// Handler.OnOpen is intentionally Task 9. The Session is adopted and
	// observable, but application callback delivery is not enabled yet.
	s.state = epollSessionOpening
}

func (s *epollSession) finalizeReactor(r *epollReactor) {
	if s == nil || s.state == epollSessionClosed || s.codecSetupOutstanding() {
		return
	}
	if s.registered {
		_ = r.poller.Del(s.fd)
		if r.resources[s.id] == s {
			delete(r.resources, s.id)
		}
		s.registered = false
	} else if r.resources[s.id] == s {
		delete(r.resources, s.id)
	}
	if s.fd >= 0 {
		_ = unix.Close(s.fd)
		s.fd = -1
	}
	if s.lease != nil {
		s.lease.release()
		s.lease = nil
	}
	if s.dial != nil {
		s.dial.finish(nil, ErrClosed)
	}
	s.state = epollSessionClosed
	s.doneOnce.Do(func() { close(s.done) })
	if s.engine != nil {
		s.engine.removeManaged(s.id)
	}
}
