//go:build linux

package transport

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
	"golang.org/x/sys/unix"
)

type epollPacketState uint8

const (
	epollPacketCreating epollPacketState = iota + 1
	epollPacketActive
	epollPacketTerminal
	epollPacketClosed
)

type epollPacketConn struct {
	engine  *epollEngine
	reactor *epollReactor
	node    epollInboxNode

	id         uint64
	fd         int
	state      epollPacketState
	registered bool
	endpoint   ogrenet.Endpoint
	bindAddr   *net.UDPAddr
	local      *net.UDPAddr
	remote     *net.UDPAddr
	handler    ogrenet.PacketHandler
	lease      *connectionLease
	stats      *packetCounters

	maxPacket int
	queue     chan packetOutbound
	quota     *byteQuota
	gate      *sendGate
	slots     chan struct{}
	closing   chan struct{}
	closeOnce sync.Once

	writeCurrent    packetOutbound
	writeActive     bool
	writeBlocked    bool
	writeInterested bool
	writeGen        uint64

	readReady   bool
	readScratch []byte

	callbackState     epollPacketCallbackState
	callbackMu        sync.Mutex
	callbackCompleted bool
	terminalPrepared  bool

	result     chan error
	resultOnce sync.Once

	closeRequested atomic.Bool

	errMu sync.RWMutex
	err   error

	ctxMu   sync.Mutex
	ctxStop func() bool

	done     chan struct{}
	doneOnce sync.Once

	// Package tests set these only before publishing work. Caller hooks run at
	// queue ownership transfer; reactor hooks stay on the owning reactor.
	testAfterPacketQueueTransfer func(packetOutbound)
	testWriteDatagram            func(packetOutbound) (int, error)
	testBeforePhysicalClose      func(*epollPacketConn)
}

func (e *epollEngine) listenNativeUDP(ctx context.Context, endpoint ogrenet.Endpoint, handler ogrenet.PacketHandler) (*epollPacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolMismatch
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()

	bindAddr, err := resolveNativeUDP(ctx, endpoint, true)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, classifyOperational(OpListen, ogrenet.SchemeUDP, nil, nil, err, hintNone)
	}
	id, err := e.nextResourceID()
	if err != nil {
		return nil, err
	}
	reactor := e.selectReactor()
	if reactor == nil {
		return nil, ErrClosed
	}
	lease, err := e.admission.acquireConnection("")
	if err != nil {
		return nil, err
	}
	p := newEpollPacketConn(e, reactor, id, endpoint, bindAddr, nil, normalizePacketHandler(handler), lease)
	if err := e.addManaged(p); err != nil {
		lease.release()
		return nil, err
	}
	reactor.signal(p)

	select {
	case createErr := <-p.result:
		if createErr != nil {
			return nil, createErr
		}
		if cause := context.Cause(ctx); cause != nil {
			_ = p.Close()
			return nil, cause
		}
		stop := context.AfterFunc(ctx, func() { _ = p.Close() })
		p.setContextStop(stop)
		return p, nil
	case <-ctx.Done():
		_ = p.Close()
		return nil, context.Cause(ctx)
	}
}

func (e *epollEngine) dialNativeUDP(ctx context.Context, endpoint ogrenet.Endpoint, handler ogrenet.PacketHandler) (*epollPacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolMismatch
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()

	dctx, cancel := boundedOperationContext(ctx, e.cfg.timeouts.Connect)
	defer cancel()
	remote, err := resolveNativeUDP(dctx, endpoint, false)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, err)
		return nil, classifyOperational(OpDial, ogrenet.SchemeUDP, nil, nil, mapped, hintNone)
	}
	id, err := e.nextResourceID()
	if err != nil {
		return nil, err
	}
	reactor := e.selectReactor()
	if reactor == nil {
		return nil, ErrClosed
	}
	lease, err := e.admission.acquireConnection(peerKey(remote))
	if err != nil {
		return nil, err
	}
	p := newEpollPacketConn(e, reactor, id, endpoint, nil, remote, normalizePacketHandler(handler), lease)
	if err := e.addManaged(p); err != nil {
		lease.release()
		return nil, err
	}
	reactor.signal(p)

	select {
	case createErr := <-p.result:
		if createErr != nil {
			return nil, createErr
		}
		if cause := context.Cause(ctx); cause != nil {
			_ = p.Close()
			return nil, cause
		}
		if cause := context.Cause(dctx); cause != nil {
			_ = p.Close()
			mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, cause)
			return nil, classifyOperational(OpDial, ogrenet.SchemeUDP, cloneUDPAddr(p.local), cloneUDPAddr(p.remote), mapped, hintNone)
		}
		return p, nil
	case <-dctx.Done():
		_ = p.Close()
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, context.Cause(dctx))
		return nil, classifyOperational(OpDial, ogrenet.SchemeUDP, nil, cloneUDPAddr(remote), mapped, hintNone)
	}
}

func newEpollPacketConn(e *epollEngine, r *epollReactor, id uint64, endpoint ogrenet.Endpoint, bindAddr, remote *net.UDPAddr, handler ogrenet.PacketHandler, lease *connectionLease) *epollPacketConn {
	p := &epollPacketConn{
		engine:   e,
		reactor:  r,
		id:       id,
		fd:       -1,
		state:    epollPacketCreating,
		endpoint: endpoint,
		bindAddr: cloneUDPAddr(bindAddr),
		remote:   cloneUDPAddr(remote),
		handler:  handler,
		lease:    lease,
		stats:    newPacketCounters(),
		result:   make(chan error, 1),
		done:     make(chan struct{}),
	}
	p.node.owner = p
	p.initNativePacketSendState()
	p.initNativePacketReadCallbacks()
	return p
}

func (p *epollPacketConn) inboxNode() *epollInboxNode { return &p.node }
func (p *epollPacketConn) resourceID() uint64         { return p.id }
func (p *epollPacketConn) resourceFD() int            { return p.fd }
func (p *epollPacketConn) managedID() uint64          { return p.id }
func (p *epollPacketConn) managedKind() epollManagedKind {
	return epollManagedPacket
}

func (p *epollPacketConn) prepareEngineDrain() {
	if p != nil && p.lease != nil {
		p.lease.beginDrain()
	}
}

func (p *epollPacketConn) requestEngineShutdown() { _ = p.Close() }
func (p *epollPacketConn) requestEngineAbort(abortReason) {
	_ = p.Close()
}

func (p *epollPacketConn) Protocol() ogrenet.Scheme { return ogrenet.SchemeUDP }

func (p *epollPacketConn) Endpoint() ogrenet.Endpoint {
	if p == nil {
		return ogrenet.Endpoint{}
	}
	return p.endpoint
}

func (p *epollPacketConn) LocalAddr() net.Addr {
	if p == nil {
		return nil
	}
	return cloneUDPAddr(p.local)
}

func (p *epollPacketConn) RemoteAddr() net.Addr {
	if p == nil || p.remote == nil {
		return nil
	}
	return cloneUDPAddr(p.remote)
}

func (p *epollPacketConn) Stats() ogrenet.PacketConnStats {
	if p == nil {
		return ogrenet.PacketConnStats{}
	}
	out := ogrenet.PacketConnStats{
		ResourceID: p.id,
		Protocol:   ogrenet.SchemeUDP,
		Local:      p.LocalAddr(),
		Remote:     p.RemoteAddr(),
	}
	if p.stats != nil {
		out.Age = p.stats.age.current()
		out.BytesRX = p.stats.bytesRX.Load()
		out.BytesTX = p.stats.bytesTX.Load()
		out.PacketsRX = p.stats.packetsRX.Load()
		out.PacketsTX = p.stats.packetsTX.Load()
		out.Backpressure = p.stats.backpressure.Load()
		out.DroppedDatagrams = p.stats.droppedDatagrams.Load()
	}
	if p.slots != nil {
		out.QueuedPackets = uint64(len(p.slots))
	}
	if p.quota != nil {
		out.QueuedBytes = nonNegativeUint64(p.quota.current())
	}
	return out
}

func (p *epollPacketConn) Done() <-chan struct{} {
	if p == nil {
		return nil
	}
	return p.done
}

func (p *epollPacketConn) Err() error {
	if p == nil {
		return nil
	}
	p.errMu.RLock()
	defer p.errMu.RUnlock()
	return p.err
}

func (p *epollPacketConn) Close() error {
	if p == nil {
		return nil
	}
	select {
	case <-p.done:
		return nil
	default:
	}
	p.closeNativePacketAdmission()
	p.closeRequested.Store(true)
	if p.reactor != nil {
		p.reactor.signal(p)
	}
	return nil
}

func (p *epollPacketConn) setContextStop(stop func() bool) {
	if p == nil || stop == nil {
		return
	}
	p.ctxMu.Lock()
	p.ctxStop = stop
	p.ctxMu.Unlock()
	select {
	case <-p.done:
		stop()
	default:
	}
}

func (p *epollPacketConn) stopContextCallback() {
	if p == nil {
		return
	}
	p.ctxMu.Lock()
	stop := p.ctxStop
	p.ctxStop = nil
	p.ctxMu.Unlock()
	if stop != nil {
		stop()
	}
}

func (p *epollPacketConn) finishCreate(err error) {
	p.resultOnce.Do(func() { p.result <- err })
}

func (p *epollPacketConn) setTerminalError(err error) {
	if p == nil || err == nil {
		return
	}
	p.errMu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.errMu.Unlock()
}

func (p *epollPacketConn) onReactorInbox(r *epollReactor) {
	if p == nil || r == nil || p.state == epollPacketClosed {
		return
	}
	if p.state == epollPacketCreating {
		if p.closeRequested.Load() {
			p.finishCreate(ErrClosed)
			p.finalizeReactor(r)
			return
		}
		if err := p.createOnReactor(r); err != nil {
			p.setTerminalError(err)
			p.finishCreate(err)
			p.finalizeReactor(r)
			return
		}
		if p.closeRequested.Load() {
			p.finishCreate(ErrClosed)
			p.finalizeReactor(r)
			return
		}
		p.state = epollPacketActive
		p.finishCreate(nil)
		return
	}
	p.consumeNativePacketCallbackCompletion()
	if p.closeRequested.Load() {
		p.finalizeReactor(r)
		return
	}
	if p.state == epollPacketActive {
		p.driveNativePacketWrite(r)
		p.driveNativePacketRead(r)
	}
}

func (p *epollPacketConn) onReactorEvent(r *epollReactor, events epoll.Events) {
	if p == nil || r == nil || p.state != epollPacketActive {
		return
	}
	if events&epoll.Readable != 0 {
		p.readReady = true
	}
	if events&(epoll.Writable|epoll.Error|epoll.Hangup) != 0 {
		p.writeBlocked = false
	}
	if p.closeRequested.Load() {
		p.finalizeReactor(r)
		return
	}
	p.driveNativePacketWrite(r)
	p.driveNativePacketRead(r)
}

func (p *epollPacketConn) onReactorRunnable(r *epollReactor) {
	if p == nil || r == nil || p.state == epollPacketClosed {
		return
	}
	if p.closeRequested.Load() {
		p.finalizeReactor(r)
		return
	}
	if p.state == epollPacketActive {
		p.driveNativePacketWrite(r)
		p.driveNativePacketRead(r)
	}
}

func (p *epollPacketConn) createOnReactor(r *epollReactor) error {
	addr := p.bindAddr
	if p.remote != nil {
		addr = p.remote
	}
	sa, family, err := nativeUDPAddrToSockaddr(addr)
	if err != nil {
		return p.operationalError(createPacketOp(p.remote), err, p.remote)
	}
	fd, err := unix.Socket(family, unix.SOCK_DGRAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return p.operationalError(createPacketOp(p.remote), err, p.remote)
	}
	closeFD := true
	defer func() {
		if closeFD {
			_ = unix.Close(fd)
		}
	}()

	if p.remote == nil {
		if err := unix.Bind(fd, sa); err != nil {
			return p.operationalError(OpListen, err, p.bindAddr)
		}
	} else if err := unix.Connect(fd, sa); err != nil {
		return p.operationalError(OpDial, err, p.remote)
	}
	local, err := nativeUDPSocketAddr(fd, false)
	if err != nil {
		return p.operationalError(createPacketOp(p.remote), err, p.remote)
	}
	remote := p.remote
	if p.remote != nil {
		remote, err = nativeUDPSocketAddr(fd, true)
		if err != nil {
			return p.operationalError(OpDial, err, p.remote)
		}
	}

	p.fd = fd
	p.local = local
	p.remote = cloneUDPAddr(remote)
	if p.remote == nil {
		p.endpoint = boundEndpoint(p.endpoint, local)
	}
	if err := r.registerResource(p); err != nil {
		p.fd = -1
		return err
	}
	if err := r.addFD(fd, epoll.Readable|epoll.Error|epoll.EdgeTriggered, p.id); err != nil {
		delete(r.resources, p.id)
		p.fd = -1
		return p.operationalError(createPacketOp(p.remote), err, p.remote)
	}
	p.registered = true
	closeFD = false
	return nil
}

func createPacketOp(remote *net.UDPAddr) Op {
	if remote == nil {
		return OpListen
	}
	return OpDial
}

func (p *epollPacketConn) operationalError(op Op, cause error, peer net.Addr) error {
	if peer == nil {
		peer = p.RemoteAddr()
	}
	return classifyOperational(op, ogrenet.SchemeUDP, p.LocalAddr(), peer, cause, hintNone)
}

func (p *epollPacketConn) finalizeReactor(r *epollReactor) {
	if p == nil || r == nil || p.state == epollPacketClosed {
		return
	}
	wasCreating := p.state == epollPacketCreating
	p.closeNativePacketAdmission()
	p.closeRequested.Store(true)
	p.state = epollPacketTerminal
	if p.gate != nil && !isClosedSignal(p.gate.done()) {
		return
	}
	if !p.terminalPrepared {
		p.releaseNativePacketWriteOwnership(ErrClosed)
		if p.registered {
			_ = r.poller.Del(p.fd)
			p.registered = false
		}
		if p.fd >= 0 {
			if p.testBeforePhysicalClose != nil {
				p.testBeforePhysicalClose(p)
			}
			_ = unix.Close(p.fd)
			p.fd = -1
		}
		if p.stats != nil {
			p.stats.age.freeze()
		}
		p.stopContextCallback()
		if wasCreating {
			p.callbackState = epollPacketCallbackClosed
		} else {
			p.observeNativePacket(ogrenet.EventClose, 0, nil, p.Err())
		}
		p.terminalPrepared = true
	}

	p.driveNativePacketCallbackState(r)
	if p.callbackState != epollPacketCallbackClosed {
		return
	}
	if r.resources[p.id] == p {
		delete(r.resources, p.id)
	}
	if p.lease != nil {
		p.lease.release()
		p.lease = nil
	}
	p.state = epollPacketClosed
	p.doneOnce.Do(func() { close(p.done) })
	if p.engine != nil {
		p.engine.removeManaged(p.id)
	}
}

var _ ogrenet.PacketConn = (*epollPacketConn)(nil)
