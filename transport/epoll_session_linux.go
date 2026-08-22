//go:build linux

package transport

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
	"github.com/qigao/ogrenet/wire"
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

	id       uint64
	fd       int
	state    epollSessionState
	endpoint ogrenet.Endpoint
	local    *net.TCPAddr
	remote   *net.TCPAddr
	handler  ogrenet.Handler
	lease    *connectionLease
	parent   *epollListener

	framer     wire.Framer
	wireFramer bool

	setupMu        sync.Mutex
	setupDone      bool
	setupSubmitted bool
	setupErr       error
	setupFramer    wire.Framer

	engineAbort      atomic.Uint32
	shutdownRequested atomic.Bool
	done             chan struct{}
	doneOnce         sync.Once
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
	if s == nil {
		return
	}
	if s.engineAbort.Load() != uint32(abortNone) {
		s.state = epollSessionTerminal
		return
	}
	if s.shutdownRequested.Load() && s.state < epollSessionActive {
		s.state = epollSessionTerminal
		return
	}
	if s.state == epollSessionCodecSetup && s.codecSetupComplete() {
		s.applyCodecSetup()
	}
}

func (s *epollSession) onReactorEvent(*epollReactor, epoll.Events) {}

func (s *epollSession) onReactorRunnable(r *epollReactor) {
	if s == nil || r == nil || s.state != epollSessionCodecSetup {
		return
	}
	s.startCodecSetup(r)
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

func (s *epollSession) publishCodecSetup(framer wire.Framer, err error) {
	s.setupMu.Lock()
	if s.setupDone {
		s.setupMu.Unlock()
		return
	}
	s.setupFramer = framer
	s.setupErr = err
	s.setupDone = true
	s.setupSubmitted = false
	s.setupMu.Unlock()
	if s.reactor != nil {
		s.reactor.signal(s)
	}
}

func (s *epollSession) codecSetupComplete() bool {
	if s == nil {
		return false
	}
	s.setupMu.Lock()
	defer s.setupMu.Unlock()
	return s.setupDone
}

func (s *epollSession) applyCodecSetup() {
	s.setupMu.Lock()
	framer := s.setupFramer
	err := s.setupErr
	s.setupMu.Unlock()
	if err != nil || framer == nil {
		s.state = epollSessionTerminal
		return
	}
	s.framer = framer
	_, s.wireFramer = framer.(*wire.Codec)
	s.state = epollSessionOpening
}
