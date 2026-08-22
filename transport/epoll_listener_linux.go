//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
	"golang.org/x/sys/unix"
)

type epollListener struct {
	engine  *epollEngine
	reactor *epollReactor
	node    epollInboxNode

	id       uint64
	fd       int
	endpoint ogrenet.Endpoint
	bindAddr *net.TCPAddr
	bound    *net.TCPAddr
	handler  ogrenet.Handler
	capacity *listenerCapacity
	stats    *listenerCounters

	result     chan error
	resultOnce sync.Once

	createAttempted bool // reactor only
	created         bool // reactor only
	registered      bool // reactor only
	finalized       bool // reactor only

	closeRequested atomic.Bool

	errMu sync.Mutex
	err   error

	ctxMu   sync.Mutex
	ctxStop func() bool

	done     chan struct{}
	doneOnce sync.Once
}

func (e *epollEngine) listenNativeTCP(ctx context.Context, endpoint ogrenet.Endpoint, handler ogrenet.Handler) (*epollListener, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
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

	bindAddr, err := resolveNativeListenTCP(ctx, endpoint, nil)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, classifyOperational(OpListen, ogrenet.SchemeTCP, nil, nil, err, hintNone)
	}
	if cause := context.Cause(ctx); cause != nil {
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
	l := &epollListener{
		engine:   e,
		reactor:  reactor,
		id:       id,
		fd:       -1,
		endpoint: endpoint,
		bindAddr: cloneTCPAddr(bindAddr),
		handler:  handler,
		capacity: newListenerCapacity(e.cfg.limits.MaxConnectionsPerListener),
		stats:    newListenerCounters(),
		result:   make(chan error, 1),
		done:     make(chan struct{}),
	}
	l.node.owner = l
	if err := e.addManaged(l); err != nil {
		return nil, err
	}
	reactor.signal(l)

	select {
	case createErr := <-l.result:
		if createErr != nil {
			return nil, createErr
		}
		if cause := context.Cause(ctx); cause != nil {
			_ = l.Close()
			return nil, cause
		}
		stop := context.AfterFunc(ctx, func() { _ = l.Close() })
		l.setContextStop(stop)
		return l, nil
	case <-ctx.Done():
		_ = l.Close()
		return nil, context.Cause(ctx)
	}
}

func (l *epollListener) inboxNode() *epollInboxNode { return &l.node }
func (l *epollListener) resourceID() uint64         { return l.id }
func (l *epollListener) resourceFD() int            { return l.fd }
func (l *epollListener) managedID() uint64          { return l.id }
func (l *epollListener) managedKind() epollManagedKind {
	return epollManagedListener
}
func (l *epollListener) prepareEngineDrain()    {}
func (l *epollListener) requestEngineShutdown() { _ = l.Close() }
func (l *epollListener) requestEngineAbort(abortReason) {
	_ = l.Close()
}

func (l *epollListener) Endpoint() ogrenet.Endpoint {
	if l == nil {
		return ogrenet.Endpoint{}
	}
	return l.endpoint
}

func (l *epollListener) Addr() net.Addr {
	if l == nil {
		return nil
	}
	if l.bound != nil {
		return cloneTCPAddr(l.bound)
	}
	return cloneTCPAddr(l.bindAddr)
}

func (l *epollListener) Stats() ogrenet.ListenerStats {
	if l == nil {
		return ogrenet.ListenerStats{}
	}
	out := ogrenet.ListenerStats{
		ResourceID: l.id,
		Protocol:   ogrenet.SchemeTCP,
		Local:      l.Addr(),
	}
	if l.stats != nil {
		out.Age = l.stats.age.current()
		out.AcceptedConnections = l.stats.accepted.Load()
	}
	if l.capacity != nil {
		out.RejectedConnections = l.capacity.rejected.Load()
		out.CurrentConnections = nonNegativeUint64(l.capacity.current())
	}
	return out
}

func (l *epollListener) Done() <-chan struct{} {
	if l == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return l.done
}

func (l *epollListener) Err() error {
	if l == nil {
		return nil
	}
	l.errMu.Lock()
	defer l.errMu.Unlock()
	return l.err
}

func (l *epollListener) Close() error {
	if l == nil {
		return nil
	}
	l.closeRequested.Store(true)
	if l.reactor != nil {
		l.reactor.signal(l)
	}
	return nil
}

func (l *epollListener) setContextStop(stop func() bool) {
	if l == nil || stop == nil {
		return
	}
	l.ctxMu.Lock()
	l.ctxStop = stop
	l.ctxMu.Unlock()
	select {
	case <-l.done:
		stop()
	default:
	}
}

func (l *epollListener) stopContextCallback() {
	l.ctxMu.Lock()
	stop := l.ctxStop
	l.ctxStop = nil
	l.ctxMu.Unlock()
	if stop != nil {
		stop()
	}
}

func (l *epollListener) finishCreate(err error) {
	l.resultOnce.Do(func() { l.result <- err })
}

func (l *epollListener) setTerminalError(err error) {
	if err == nil {
		return
	}
	l.errMu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.errMu.Unlock()
}

func (l *epollListener) onReactorInbox(r *epollReactor) {
	if l == nil || r == nil || l.finalized {
		return
	}
	if !l.createAttempted {
		l.createAttempted = true
		if l.closeRequested.Load() {
			l.finishCreate(ErrClosed)
			l.finalizeReactor(r)
			return
		}
		if err := l.createOnReactor(r); err != nil {
			l.finishCreate(err)
			l.finalizeReactor(r)
			return
		}
		if l.closeRequested.Load() {
			l.finishCreate(ErrClosed)
			l.finalizeReactor(r)
			return
		}
		l.finishCreate(nil)
		return
	}
	if l.closeRequested.Load() {
		l.finalizeReactor(r)
	}
}

func (l *epollListener) onReactorEvent(r *epollReactor, events epoll.Events) {
	if l == nil || l.finalized || !l.created {
		return
	}
	if events&(epoll.Readable|epoll.Error|epoll.Hangup) != 0 {
		l.acceptReady(r)
	}
}

func (l *epollListener) onReactorRunnable(r *epollReactor) {
	if l == nil || l.finalized || !l.created {
		return
	}
	l.acceptReady(r)
}

func (l *epollListener) createOnReactor(r *epollReactor) error {
	sa, family, err := nativeTCPAddrToSockaddr(l.bindAddr)
	if err != nil {
		return err
	}
	fd, err := unix.Socket(family, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return classifyOperational(OpListen, ogrenet.SchemeTCP, l.bindAddr, nil, err, hintNone)
	}
	closeFD := true
	defer func() {
		if closeFD {
			_ = unix.Close(fd)
		}
	}()
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return classifyOperational(OpListen, ogrenet.SchemeTCP, l.bindAddr, nil, err, hintNone)
	}
	if err := unix.Bind(fd, sa); err != nil {
		return classifyOperational(OpListen, ogrenet.SchemeTCP, l.bindAddr, nil, err, hintNone)
	}
	if err := unix.Listen(fd, unix.SOMAXCONN); err != nil {
		return classifyOperational(OpListen, ogrenet.SchemeTCP, l.bindAddr, nil, err, hintNone)
	}
	bound, err := nativeSocketAddr(fd, false)
	if err != nil {
		return classifyOperational(OpListen, ogrenet.SchemeTCP, l.bindAddr, nil, err, hintNone)
	}
	l.fd = fd
	l.bound = bound
	if err := r.registerResource(l); err != nil {
		l.fd = -1
		return err
	}
	if err := r.poller.Add(fd, epoll.Readable|epoll.Error|epoll.EdgeTriggered, l.id); err != nil {
		delete(r.resources, l.id)
		l.fd = -1
		return classifyOperational(OpListen, ogrenet.SchemeTCP, bound, nil, err, hintNone)
	}
	l.registered = true
	l.created = true
	closeFD = false
	return nil
}

func (l *epollListener) acceptReady(r *epollReactor) {
	budget := r.cfg.ioBudgetOps
	if budget <= 0 {
		budget = 1
	}
	for accepted := 0; accepted < budget; {
		fd, peer, err := unix.Accept4(l.fd, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				return
			}
			l.setTerminalError(classifyOperational(OpAccept, ogrenet.SchemeTCP, l.bound, nil, err, hintNone))
			l.closeRequested.Store(true)
			l.finalizeReactor(r)
			return
		}
		accepted++
		l.adoptAcceptedFD(fd, peer)
	}
	if !l.finalized && l.created {
		r.requeue(l)
	}
}

func (l *epollListener) adoptAcceptedFD(fd int, peer unix.Sockaddr) {
	remote, err := nativeSockaddrToTCPAddr(peer)
	if err != nil {
		_ = unix.Close(fd)
		return
	}
	local, err := nativeSocketAddr(fd, false)
	if err != nil {
		_ = unix.Close(fd)
		return
	}
	lease, err := l.engine.admission.acquireOpeningWithListener(peerKey(remote), l.capacity)
	if err != nil {
		_ = unix.Close(fd)
		return
	}
	if err := configureNativeTCP(fd, l.engine.cfg.tcp); err != nil {
		lease.release()
		_ = unix.Close(fd)
		return
	}
	id, err := l.engine.nextResourceID()
	if err != nil {
		lease.release()
		_ = unix.Close(fd)
		return
	}
	target := l.engine.selectReactor()
	if target == nil {
		lease.release()
		_ = unix.Close(fd)
		return
	}
	session := newEpollBootstrapSession(l.engine, target, id, fd, l.endpoint, local, remote, l.handler, lease, l)
	session.state = epollSessionHandoff
	if err := l.engine.addManaged(session); err != nil {
		lease.release()
		_ = unix.Close(fd)
		return
	}
	// The listener reactor still logically owns the accepted fd until the target
	// reactor's Poller.Add succeeds. The target handles the delegated close and
	// lease release exactly once on handoff failure.
	target.signal(session)
}

func (l *epollListener) finalizeReactor(r *epollReactor) {
	if l == nil || l.finalized {
		return
	}
	l.finalized = true
	wasCreated := l.created
	if l.registered {
		_ = r.poller.Del(l.fd)
		if r.resources[l.id] == l {
			delete(r.resources, l.id)
		}
		l.registered = false
	} else if r.resources[l.id] == l {
		delete(r.resources, l.id)
	}
	if l.fd >= 0 {
		_ = unix.Close(l.fd)
		l.fd = -1
	}
	if l.stats != nil {
		l.stats.age.freeze()
	}
	l.stopContextCallback()
	if wasCreated && l.engine != nil && l.engine.observer != nil {
		l.engine.observer.emit(ogrenet.Event{
			Kind:       ogrenet.EventClose,
			Resource:   ogrenet.ResourceListener,
			ResourceID: l.id,
			Protocol:   ogrenet.SchemeTCP,
			Local:      l.Addr(),
			Err:        l.Err(),
		})
	}
	l.doneOnce.Do(func() { close(l.done) })
	if l.engine != nil {
		l.engine.removeManaged(l.id)
	}
}
