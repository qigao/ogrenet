//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
	"golang.org/x/sys/unix"
)

type epollDialResult struct {
	session *epollSession
	err     error
}

type epollDialState struct {
	addrs    []*net.TCPAddr
	nextAddr int
	result   chan epollDialResult
	once     sync.Once

	observerStart time.Time
	deadline      time.Time
	connectGen    uint64
	lastErr       error
	connected     bool

	cancelMu  sync.Mutex
	cancelErr error
}

func resolveNativeDialTCP(ctx context.Context, endpoint ogrenet.Endpoint, resolver nativeIPResolver) ([]*net.TCPAddr, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if endpoint.Scheme != ogrenet.SchemeTCP {
		return nil, ErrProtocolMismatch
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if ip := net.ParseIP(endpoint.Host); ip != nil {
		return []*net.TCPAddr{{IP: append(net.IP(nil), ip...), Port: int(endpoint.Port)}}, nil
	}
	if resolver == nil {
		resolver = netNativeIPResolver{}
	}
	addrs, err := resolver.LookupIPAddr(ctx, endpoint.Host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errNativeNoResolvedAddress
	}
	out := make([]*net.TCPAddr, 0, len(addrs))
	for _, addr := range addrs {
		if addr.IP == nil {
			continue
		}
		out = append(out, &net.TCPAddr{
			IP:   append(net.IP(nil), addr.IP...),
			Port: int(endpoint.Port),
			Zone: addr.Zone,
		})
	}
	if len(out) == 0 {
		return nil, errNativeNoResolvedAddress
	}
	return out, nil
}

func (e *epollEngine) dialNativeTCP(ctx context.Context, endpoint ogrenet.Endpoint, handler ogrenet.Handler, resolver nativeIPResolver) (*epollSession, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if endpoint.Scheme != ogrenet.SchemeTCP {
		return nil, ErrProtocolUnsupported
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()

	observing := e.observer != nil
	var observerStart time.Time
	if observing {
		observerStart = time.Now()
	}
	dctx, cancel := boundedOperationContext(ctx, e.cfg.timeouts.Connect)
	defer cancel()
	addrs, err := resolveNativeDialTCP(dctx, endpoint, resolver)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			if observing {
				e.emitNativeConnect(0, endpoint, nil, nil, observerStart, cause)
			}
			return nil, cause
		}
		mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, err)
		opErr := classifyOperational(OpDial, ogrenet.SchemeTCP, nil, nil, mapped, hintNone)
		if observing {
			e.emitNativeConnect(0, endpoint, nil, nil, observerStart, opErr)
		}
		return nil, opErr
	}
	if cause := context.Cause(ctx); cause != nil {
		if observing {
			e.emitNativeConnect(0, endpoint, nil, nil, observerStart, cause)
		}
		return nil, cause
	}

	id, err := e.nextResourceID()
	if err != nil {
		return nil, err
	}
	reactor := e.selectReactor()
	if reactor == nil {
		return nil, ErrClosed
	}
	deadline, _ := dctx.Deadline()
	s := newEpollBootstrapSession(e, reactor, id, -1, endpoint, nil, nil, handler, nil, nil)
	s.state = epollSessionConnecting
	s.dial = &epollDialState{
		addrs:         cloneNativeDialAddrs(addrs),
		result:        make(chan epollDialResult, 1),
		observerStart: observerStart,
		deadline:      deadline,
	}
	if err := e.addManaged(s); err != nil {
		return nil, err
	}
	reactor.signal(s)

	select {
	case result := <-s.dial.result:
		return result.session, result.err
	case <-dctx.Done():
		select {
		case result := <-s.dial.result:
			return result.session, result.err
		default:
		}
		var resultErr error
		if cause := context.Cause(ctx); cause != nil {
			resultErr = cause
		} else {
			mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, context.Cause(dctx))
			resultErr = classifyOperational(OpDial, ogrenet.SchemeTCP, s.local, s.remote, mapped, hintNone)
		}
		s.dial.publishCancel(resultErr)
		reactor.signal(s)
		return nil, resultErr
	}
}

func cloneNativeDialAddrs(addrs []*net.TCPAddr) []*net.TCPAddr {
	out := make([]*net.TCPAddr, 0, len(addrs))
	for _, addr := range addrs {
		out = append(out, cloneTCPAddr(addr))
	}
	return out
}

func (d *epollDialState) publishCancel(err error) {
	if d == nil || err == nil {
		return
	}
	d.cancelMu.Lock()
	if d.cancelErr == nil {
		d.cancelErr = err
	}
	d.cancelMu.Unlock()
}

func (d *epollDialState) cancelCause() error {
	if d == nil {
		return nil
	}
	d.cancelMu.Lock()
	defer d.cancelMu.Unlock()
	return d.cancelErr
}

func (d *epollDialState) finish(session *epollSession, err error) {
	if d == nil {
		return
	}
	d.once.Do(func() {
		d.result <- epollDialResult{session: session, err: err}
	})
}

func (e *epollEngine) emitNativeConnect(resourceID uint64, endpoint ogrenet.Endpoint, local, remote net.Addr, started time.Time, err error) {
	if e == nil || e.observer == nil {
		return
	}
	var duration time.Duration
	if !started.IsZero() {
		duration = positiveElapsed(started)
	}
	e.observer.emit(ogrenet.Event{
		Kind:       ogrenet.EventConnect,
		Resource:   ogrenet.ResourceSession,
		ResourceID: resourceID,
		Protocol:   endpoint.Scheme,
		Local:      local,
		Remote:     remote,
		Duration:   duration,
		Err:        err,
	})
}

func (s *epollSession) driveNativeConnect(r *epollReactor) {
	if s == nil || s.dial == nil || r == nil || s.state != epollSessionConnecting {
		return
	}
	if cancelErr := s.dial.cancelCause(); cancelErr != nil {
		s.failNativeDial(r, cancelErr, false)
		return
	}
	if r.resources[s.id] == nil {
		if err := r.registerResource(s); err != nil {
			s.failNativeDial(r, err, false)
			return
		}
	}
	if s.registered && s.fd >= 0 {
		return
	}

	for s.dial.nextAddr < len(s.dial.addrs) {
		if cancelErr := s.dial.cancelCause(); cancelErr != nil {
			s.failNativeDial(r, cancelErr, false)
			return
		}
		addr := s.dial.addrs[s.dial.nextAddr]
		s.dial.nextAddr++
		sa, family, err := nativeTCPAddrToSockaddr(addr)
		if err != nil {
			s.dial.lastErr = err
			continue
		}
		fd, err := unix.Socket(family, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			s.dial.lastErr = err
			continue
		}
		s.fd = fd
		err = unix.Connect(fd, sa)
		switch {
		case err == nil, errors.Is(err, unix.EISCONN):
			s.finishNativeTransportConnect(r)
			return
		case errors.Is(err, unix.EINPROGRESS), errors.Is(err, unix.EALREADY), errors.Is(err, unix.EINTR):
			if err := r.addFD(fd, epoll.Writable|epoll.Error|epoll.EdgeTriggered, s.id); err != nil {
				s.closeNativeConnectAttempt(r)
				s.dial.lastErr = err
				continue
			}
			s.registered = true
			s.dial.connectGen++
			if !s.dial.deadline.IsZero() {
				r.scheduleDeadline(s.id, epollDeadlineConnect, s.dial.connectGen, s.dial.deadline)
			}
			return
		default:
			s.dial.lastErr = err
			s.closeNativeConnectAttempt(r)
		}
	}

	cause := s.dial.lastErr
	if cause == nil {
		cause = errNativeNoResolvedAddress
	}
	opErr := classifyOperational(OpDial, ogrenet.SchemeTCP, nil, nil, cause, hintNone)
	s.failNativeDial(r, opErr, false)
}

func (s *epollSession) onNativeConnectEvent(r *epollReactor, events epoll.Events) {
	if s == nil || s.dial == nil || s.state != epollSessionConnecting || s.fd < 0 {
		return
	}
	if events&(epoll.Writable|epoll.Error|epoll.Hangup|epoll.PeerClosed) == 0 {
		return
	}
	if cancelErr := s.dial.cancelCause(); cancelErr != nil {
		s.failNativeDial(r, cancelErr, false)
		return
	}
	soErr, err := unix.GetsockoptInt(s.fd, unix.SOL_SOCKET, unix.SO_ERROR)
	if err == nil && soErr != 0 {
		err = syscall.Errno(soErr)
	}
	if err != nil {
		s.dial.lastErr = err
		s.closeNativeConnectAttempt(r)
		s.driveNativeConnect(r)
		return
	}
	s.finishNativeTransportConnect(r)
}

func (s *epollSession) closeNativeConnectAttempt(r *epollReactor) {
	if s == nil {
		return
	}
	if s.registered && s.fd >= 0 {
		_ = r.poller.Del(s.fd)
		s.registered = false
	}
	if s.fd >= 0 {
		_ = unix.Close(s.fd)
		s.fd = -1
	}
	if s.dial != nil {
		s.dial.connectGen++
	}
}

func (s *epollSession) finishNativeTransportConnect(r *epollReactor) {
	if s == nil || s.dial == nil || s.fd < 0 {
		return
	}
	s.dial.connectGen++
	if s.registered {
		if err := r.poller.Mod(s.fd, epoll.Readable|epoll.PeerClosed|epoll.Error|epoll.EdgeTriggered, s.id); err != nil {
			s.failNativeDial(r, classifyOperational(OpDial, ogrenet.SchemeTCP, s.local, s.remote, err, hintNone), true)
			return
		}
	} else {
		if err := r.addFD(s.fd, epoll.Readable|epoll.PeerClosed|epoll.Error|epoll.EdgeTriggered, s.id); err != nil {
			s.failNativeDial(r, classifyOperational(OpDial, ogrenet.SchemeTCP, s.local, s.remote, err, hintNone), true)
			return
		}
		s.registered = true
	}
	local, err := nativeSocketAddr(s.fd, false)
	if err != nil {
		s.failNativeDial(r, classifyOperational(OpDial, ogrenet.SchemeTCP, nil, nil, err, hintNone), true)
		return
	}
	remote, err := nativeSocketAddr(s.fd, true)
	if err != nil {
		s.failNativeDial(r, classifyOperational(OpDial, ogrenet.SchemeTCP, local, nil, err, hintNone), true)
		return
	}
	s.local = cloneTCPAddr(local)
	s.remote = cloneTCPAddr(remote)
	s.dial.connected = true
	if err := configureNativeTCP(s.fd, s.engine.cfg.tcp); err != nil {
		s.failNativeDial(r, classifyOperational(OpDial, ogrenet.SchemeTCP, local, remote, err, hintNone), true)
		return
	}
	lease, err := s.engine.admission.acquireOpening(peerKey(remote))
	if err != nil {
		s.failNativeDial(r, classifyOperational(OpDial, ogrenet.SchemeTCP, local, remote, err, hintNone), true)
		return
	}
	s.lease = lease
	s.state = epollSessionCodecSetup
	r.requeue(s)
}

func (s *epollSession) failNativeDial(r *epollReactor, err error, transportConnected bool) {
	if s == nil || s.dial == nil {
		return
	}
	if transportConnected || s.dial.connected {
		s.engine.emitNativeConnect(0, s.endpoint, cloneTCPAddr(s.local), cloneTCPAddr(s.remote), s.dial.observerStart, nil)
	} else {
		s.engine.emitNativeConnect(0, s.endpoint, cloneTCPAddr(s.local), cloneTCPAddr(s.remote), s.dial.observerStart, err)
	}
	s.dial.finish(nil, err)
	s.state = epollSessionTerminal
	s.finalizeReactor(r)
}

func (s *epollSession) currentDeadlineGeneration(kind epollDeadlineKind) uint64 {
	if s == nil || s.dial == nil || kind != epollDeadlineConnect {
		return 0
	}
	return s.dial.connectGen
}

func (s *epollSession) onReactorDeadline(r *epollReactor, kind epollDeadlineKind, generation uint64) {
	if s == nil || s.dial == nil || kind != epollDeadlineConnect || s.state != epollSessionConnecting || generation != s.dial.connectGen {
		return
	}
	mapped := &TimeoutError{Kind: TimeoutConnect, Cause: context.DeadlineExceeded}
	opErr := classifyOperational(OpDial, ogrenet.SchemeTCP, cloneTCPAddr(s.local), cloneTCPAddr(s.remote), mapped, hintNone)
	s.failNativeDial(r, opErr, false)
}
