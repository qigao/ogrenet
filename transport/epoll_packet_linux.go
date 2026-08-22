//go:build linux

package transport

import (
	"context"
	"errors"
	"fmt"
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

	result     chan error
	resultOnce sync.Once

	closeRequested atomic.Bool

	errMu sync.RWMutex
	err   error

	ctxMu   sync.Mutex
	ctxStop func() bool

	done     chan struct{}
	doneOnce sync.Once

	// Package tests set this before publishing Close. It is invoked only by the
	// owning reactor immediately before the physical close(2).
	testBeforePhysicalClose func(*epollPacketConn)
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

func (p *epollPacketConn) Send(context.Context, ogrenet.Packet) error { return ErrProtocolUnsupported }
func (p *epollPacketConn) TrySend(ogrenet.Packet) error              { return ErrProtocolUnsupported }
func (p *epollPacketConn) SendTo(context.Context, net.Addr, ogrenet.Packet) error {
	return ErrProtocolUnsupported
}
func (p *epollPacketConn) TrySendTo(net.Addr, ogrenet.Packet) error { return ErrProtocolUnsupported }

func (p *epollPacketConn) Close() error {
	if p == nil {
		return nil
	}
	select {
	case <-p.done:
		return nil
	default:
	}
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
	if p.closeRequested.Load() {
		p.finalizeReactor(r)
	}
}

func (p *epollPacketConn) onReactorEvent(r *epollReactor, _ epoll.Events) {
	if p == nil || r == nil || p.state != epollPacketActive {
		return
	}
	if p.closeRequested.Load() {
		p.finalizeReactor(r)
	}
}

func (p *epollPacketConn) onReactorRunnable(r *epollReactor) {
	if p != nil && p.closeRequested.Load() {
		p.finalizeReactor(r)
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
	p.state = epollPacketTerminal
	if p.registered {
		_ = r.poller.Del(p.fd)
		if r.resources[p.id] == p {
			delete(r.resources, p.id)
		}
		p.registered = false
	} else if r.resources[p.id] == p {
		delete(r.resources, p.id)
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

func resolveNativeUDP(ctx context.Context, endpoint ogrenet.Endpoint, listen bool) (*net.UDPAddr, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolMismatch
	}
	if endpoint.Host == "" {
		if !listen {
			return nil, errors.New("transport: UDP dial host required")
		}
		return &net.UDPAddr{IP: net.IPv4zero, Port: int(endpoint.Port)}, nil
	}
	if ip := net.ParseIP(endpoint.Host); ip != nil {
		return &net.UDPAddr{IP: append(net.IP(nil), ip...), Port: int(endpoint.Port)}, nil
	}
	addrs, err := (netNativeIPResolver{}).LookupIPAddr(ctx, endpoint.Host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errNativeNoResolvedAddress
	}
	return &net.UDPAddr{IP: append(net.IP(nil), addrs[0].IP...), Port: int(endpoint.Port), Zone: addrs[0].Zone}, nil
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	out := *addr
	out.IP = append(net.IP(nil), addr.IP...)
	return &out
}

func nativeUDPAddrToSockaddr(addr *net.UDPAddr) (unix.Sockaddr, int, error) {
	if addr == nil || addr.Port < 0 || addr.Port > 65535 {
		return nil, 0, errNativeInvalidTCPAddress
	}
	ip := addr.IP
	if len(ip) == 0 {
		ip = net.IPv4zero
	}
	if v4 := ip.To4(); v4 != nil {
		sa := &unix.SockaddrInet4{Port: addr.Port}
		copy(sa.Addr[:], v4)
		return sa, unix.AF_INET, nil
	}
	v6 := ip.To16()
	if v6 == nil {
		return nil, 0, errNativeInvalidTCPAddress
	}
	sa := &unix.SockaddrInet6{Port: addr.Port}
	copy(sa.Addr[:], v6)
	if addr.Zone != "" {
		iface, err := net.InterfaceByName(addr.Zone)
		if err != nil {
			return nil, 0, err
		}
		sa.ZoneId = uint32(iface.Index)
	}
	return sa, unix.AF_INET6, nil
}

func nativeSockaddrToUDPAddr(sa unix.Sockaddr) (*net.UDPAddr, error) {
	switch x := sa.(type) {
	case *unix.SockaddrInet4:
		return &net.UDPAddr{IP: net.IPv4(x.Addr[0], x.Addr[1], x.Addr[2], x.Addr[3]), Port: x.Port}, nil
	case *unix.SockaddrInet6:
		ip := make(net.IP, net.IPv6len)
		copy(ip, x.Addr[:])
		zone := ""
		if x.ZoneId != 0 {
			if iface, err := net.InterfaceByIndex(int(x.ZoneId)); err == nil {
				zone = iface.Name
			}
		}
		return &net.UDPAddr{IP: ip, Port: x.Port, Zone: zone}, nil
	default:
		return nil, errNativeSockaddrType
	}
}

func nativeUDPSocketAddr(fd int, peer bool) (*net.UDPAddr, error) {
	var (
		sa  unix.Sockaddr
		err error
	)
	if peer {
		sa, err = unix.Getpeername(fd)
	} else {
		sa, err = unix.Getsockname(fd)
	}
	if err != nil {
		return nil, err
	}
	return nativeSockaddrToUDPAddr(sa)
}

var _ ogrenet.PacketConn = (*epollPacketConn)(nil)
