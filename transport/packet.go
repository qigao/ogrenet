package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/qigao/ogrenet"
)

type packetOutbound struct {
	peer  *net.UDPAddr
	data  []byte
	ack   chan error
	bytes int
}

type packetConn struct {
	engine    *Engine
	endpoint  ogrenet.Endpoint
	conn      *net.UDPConn
	remote    *net.UDPAddr
	handler   ogrenet.PacketHandler
	maxPacket int
	timeouts  Timeouts
	activity  *activityClock
	queue     chan packetOutbound
	quota     *byteQuota
	gate      *sendGate
	slots     chan struct{}
	drainReq  chan struct{}
	closing   chan struct{}
	done      chan struct{}

	drainOnce sync.Once
	closeOnce sync.Once
	finalOnce sync.Once
	loops     sync.WaitGroup
	errMu     sync.RWMutex
	err       error
}

func (p *packetConn) Protocol() ogrenet.Scheme   { return ogrenet.SchemeUDP }
func (p *packetConn) Endpoint() ogrenet.Endpoint { return p.endpoint }
func (p *packetConn) LocalAddr() net.Addr        { return p.conn.LocalAddr() }
func (p *packetConn) Done() <-chan struct{}      { return p.done }
func (p *packetConn) RemoteAddr() net.Addr {
	if p.remote == nil {
		return nil
	}
	return p.remote
}

func (p *packetConn) Err() error {
	p.errMu.RLock()
	defer p.errMu.RUnlock()
	return p.err
}

func (p *packetConn) Send(ctx context.Context, packet ogrenet.Packet) error {
	if p.remote == nil {
		return ErrNotConnected
	}
	return p.send(ctx, nil, packet)
}

func (p *packetConn) TrySend(packet ogrenet.Packet) error {
	if p.remote == nil {
		return ErrNotConnected
	}
	return p.trySend(nil, packet)
}

func (p *packetConn) SendTo(ctx context.Context, peer net.Addr, packet ogrenet.Packet) error {
	target, err := p.resolvePeer(peer)
	if err != nil {
		return err
	}
	return p.send(ctx, target, packet)
}

func (p *packetConn) TrySendTo(peer net.Addr, packet ogrenet.Packet) error {
	target, err := p.resolvePeer(peer)
	if err != nil {
		return err
	}
	return p.trySend(target, packet)
}

func (p *packetConn) resolvePeer(peer net.Addr) (*net.UDPAddr, error) {
	if peer == nil {
		return nil, ErrPeerRequired
	}
	var target *net.UDPAddr
	if udp, ok := peer.(*net.UDPAddr); ok {
		copyAddr := *udp
		copyAddr.IP = append(net.IP(nil), udp.IP...)
		target = &copyAddr
	} else {
		resolved, err := net.ResolveUDPAddr("udp", peer.String())
		if err != nil {
			return nil, p.operationalError(OpSend, fmt.Errorf("transport: resolve UDP peer: %w", err), nil)
		}
		target = resolved
	}
	if p.remote != nil {
		if target.String() != p.remote.String() {
			return nil, ErrPeerMismatch
		}
		return nil, nil
	}
	return target, nil
}

func (p *packetConn) send(ctx context.Context, peer *net.UDPAddr, packet ogrenet.Packet) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if !p.gate.enter() {
		return p.operationalError(OpSend, ErrClosed, peer)
	}
	defer p.gate.leave()
	if err := p.validatePacket(packet); err != nil {
		return p.operationalError(OpSend, err, peer)
	}
	if err := p.acquireSlot(ctx); err != nil {
		return p.sendError(ctx, err, peer)
	}
	held := true
	defer func() {
		if held {
			p.releaseSlot()
		}
	}()
	if err := p.quota.acquire(ctx, p.closing, len(packet.Data)); err != nil {
		return p.sendError(ctx, err, peer)
	}
	data := append([]byte(nil), packet.Data...)
	held = false

	ack := make(chan error, 1)
	req := packetOutbound{peer: peer, data: data, ack: ack, bytes: len(data)}
	select {
	case <-ctx.Done():
		p.quota.release(req.bytes)
		p.releaseSlot()
		return context.Cause(ctx)
	case <-p.closing:
		p.quota.release(req.bytes)
		p.releaseSlot()
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if terminal := p.Err(); terminal != nil {
			return terminal
		}
		return p.operationalError(OpSend, ErrClosed, peer)
	case p.queue <- req:
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-p.closing:
		select {
		case err := <-ack:
			return err
		default:
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			if terminal := p.Err(); terminal != nil {
				return terminal
			}
			return p.operationalError(OpSend, ErrClosed, peer)
		}
	}
}

func (p *packetConn) trySend(peer *net.UDPAddr, packet ogrenet.Packet) error {
	if !p.gate.enter() {
		return p.operationalError(OpSend, ErrClosed, peer)
	}
	defer p.gate.leave()
	if err := p.validatePacket(packet); err != nil {
		return p.operationalError(OpSend, err, peer)
	}
	if err := p.tryAcquireSlot(); err != nil {
		return p.operationalError(OpSend, err, peer)
	}
	held := true
	defer func() {
		if held {
			p.releaseSlot()
		}
	}()
	if err := p.quota.tryAcquire(len(packet.Data)); err != nil {
		return p.operationalError(OpSend, err, peer)
	}
	data := append([]byte(nil), packet.Data...)
	held = false
	req := packetOutbound{peer: peer, data: data, bytes: len(data)}
	select {
	case <-p.closing:
		p.quota.release(req.bytes)
		p.releaseSlot()
		if terminal := p.Err(); terminal != nil {
			return terminal
		}
		return p.operationalError(OpSend, ErrClosed, peer)
	case p.queue <- req:
		return nil
	default:
		p.quota.release(req.bytes)
		p.releaseSlot()
		return p.operationalError(OpSend, ErrWouldBlock, peer)
	}
}

func (p *packetConn) validatePacket(packet ogrenet.Packet) error {
	if len(packet.Data) > p.maxPacket {
		return ErrDatagramTooLarge
	}
	return nil
}

func (p *packetConn) Close() error {
	p.initiateClose(nil)
	return nil
}

func (p *packetConn) start() {
	loopCount := 2
	if p.activity != nil {
		loopCount++
	}
	p.loops.Add(loopCount)
	go func() {
		defer p.loops.Done()
		p.writerLoop()
	}()
	go func() {
		defer p.loops.Done()
		p.readerLoop()
	}()
	if p.activity != nil {
		go func() {
			defer p.loops.Done()
			p.activity.run(p.closing, func(kind TimeoutKind) {
				p.initiateClose(p.operationalError(OpReceive, &TimeoutError{Kind: kind}, p.remote))
			})
		}()
	}
	go func() {
		p.loops.Wait()
		p.finalize()
	}()
}

func (p *packetConn) writerLoop() {
	defer func() {
		<-p.gate.done()
		p.failPending(ErrClosed)
	}()

	draining := false
	for {
		if draining {
			select {
			case <-p.closing:
				return
			case req := <-p.queue:
				if !p.handleOutbound(req) {
					return
				}
			case <-p.gate.done():
				for {
					select {
					case req := <-p.queue:
						if !p.handleOutbound(req) {
							return
						}
					default:
						p.initiateClose(nil)
						return
					}
				}
			}
			continue
		}

		select {
		case <-p.closing:
			return
		case <-p.drainReq:
			draining = true
		case req := <-p.queue:
			if !p.handleOutbound(req) {
				return
			}
		}
	}
}

func (p *packetConn) handleOutbound(req packetOutbound) bool {
	err := p.writeDatagram(req)
	p.quota.release(req.bytes)
	p.releaseSlot()
	if err == nil {
		if req.ack != nil {
			req.ack <- nil
		}
		return true
	}

	opErr := p.operationalError(OpWrite, err, req.peer)
	p.initiateClose(opErr)
	sendErr := p.Err()
	if sendErr == nil {
		sendErr = p.operationalError(OpSend, ErrClosed, req.peer)
	}
	if req.ack != nil {
		req.ack <- sendErr
	}
	return false
}

func (p *packetConn) writeDatagram(req packetOutbound) error {
	writeTimeout := p.timeouts.Write
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}
	if err := p.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return fmt.Errorf("transport: set UDP write deadline: %w", err)
	}
	var err error
	if req.peer == nil {
		_, err = p.conn.Write(req.data)
	} else {
		_, err = p.conn.WriteToUDP(req.data, req.peer)
	}
	clearErr := p.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		if isTimeoutFailure(err) {
			return &TimeoutError{Kind: TimeoutWrite, Cause: err}
		}
		return fmt.Errorf("transport: UDP write: %w", err)
	}
	if clearErr != nil {
		return fmt.Errorf("transport: clear UDP write deadline: %w", clearErr)
	}
	if p.activity != nil {
		p.activity.touch()
	}
	return nil
}

func (p *packetConn) failPending(err error) {
	for {
		select {
		case req := <-p.queue:
			p.quota.release(req.bytes)
			p.releaseSlot()
			if req.ack != nil {
				pendingErr := p.Err()
				if pendingErr == nil {
					pendingErr = p.operationalError(OpSend, err, req.peer)
				}
				req.ack <- pendingErr
			}
		default:
			return
		}
	}
}

func (p *packetConn) readerLoop() {
	buf := make([]byte, 65535)
	for {
		if p.remote != nil && p.timeouts.ReadIdle > 0 {
			if err := p.conn.SetReadDeadline(time.Now().Add(p.timeouts.ReadIdle)); err != nil {
				p.initiateClose(p.operationalError(OpReceive, fmt.Errorf("transport: set UDP read deadline: %w", err), p.remote))
				return
			}
		}
		var (
			n    int
			peer *net.UDPAddr
			err  error
		)
		if p.remote == nil {
			n, peer, err = p.conn.ReadFromUDP(buf)
		} else {
			n, err = p.conn.Read(buf)
			peer = p.remote
		}
		if p.remote != nil && p.timeouts.ReadIdle > 0 {
			if clearErr := p.conn.SetReadDeadline(time.Time{}); clearErr != nil && err == nil {
				p.initiateClose(p.operationalError(OpReceive, fmt.Errorf("transport: clear UDP read deadline: %w", clearErr), peer))
				return
			}
		}
		if n > 0 && p.activity != nil {
			p.activity.touch()
		}
		if err != nil {
			if p.isClosing() {
				return
			}
			if p.remote != nil && p.timeouts.ReadIdle > 0 && isTimeoutFailure(err) {
				p.initiateClose(p.operationalError(OpReceive, &TimeoutError{Kind: TimeoutReadIdle, Cause: err}, peer))
			} else if normalized := normalizePacketError(err); normalized != nil {
				p.initiateClose(p.operationalError(OpReceive, normalized, peer))
			} else {
				p.initiateClose(nil)
			}
			return
		}
		if n > p.maxPacket {
			continue
		}
		data := append([]byte(nil), buf[:n]...)
		p.handler.OnPacket(p, peer, ogrenet.Packet{Data: data})
		if p.isClosing() {
			return
		}
	}
}

func (p *packetConn) acquireSlot(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closing:
		return ErrClosed
	case p.slots <- struct{}{}:
		return nil
	}
}

func (p *packetConn) tryAcquireSlot() error {
	select {
	case <-p.closing:
		return ErrClosed
	case p.slots <- struct{}{}:
		return nil
	default:
		return ErrWouldBlock
	}
}

func (p *packetConn) releaseSlot() { <-p.slots }

func (p *packetConn) initiateClose(cause error) {
	p.closeOnce.Do(func() {
		if cause != nil {
			var typed *Error
			if !errors.As(cause, &typed) {
				cause = normalizePacketError(cause)
			}
		}
		if cause != nil {
			p.errMu.Lock()
			p.err = cause
			p.errMu.Unlock()
		}
		p.gate.close()
		close(p.closing)
		_ = p.conn.Close()
	})
}

func (p *packetConn) finalize() {
	p.finalOnce.Do(func() {
		p.initiateClose(nil)
		defer func() {
			close(p.done)
			p.engine.removePacket(p)
		}()
		p.handler.OnClose(p, p.Err())
	})
}

func (p *packetConn) isClosing() bool {
	select {
	case <-p.closing:
		return true
	default:
		return false
	}
}

func normalizePacketError(err error) error {
	if err == nil || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (e *Engine) listenPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	lc := net.ListenConfig{}
	raw, err := lc.ListenPacket(ctx, "udp", endpoint.Address())
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, classifyOperational(OpListen, ogrenet.SchemeUDP, nil, nil, err, hintNone)
	}
	conn, ok := raw.(*net.UDPConn)
	if !ok {
		local := raw.LocalAddr()
		_ = raw.Close()
		return nil, classifyOperational(OpListen, ogrenet.SchemeUDP, local, nil, fmt.Errorf("transport: UDP listen returned %T", raw), hintNone)
	}
	p := e.newPacketConn(conn, boundEndpoint(endpoint, conn.LocalAddr()), nil, h)
	if err := e.addPacket(p); err != nil {
		_ = conn.Close()
		return nil, classifyOperational(OpListen, ogrenet.SchemeUDP, conn.LocalAddr(), nil, err, hintNone)
	}
	p.start()
	go func() {
		select {
		case <-ctx.Done():
			p.initiateClose(nil)
		case <-p.done:
		}
	}()
	return p, nil
}

func (e *Engine) dialPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	dialer := net.Dialer{}
	dctx, cancel := boundedOperationContext(ctx, e.cfg.timeouts.Connect)
	defer cancel()
	raw, err := dialer.DialContext(dctx, "udp", endpoint.Address())
	if err != nil {
		mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, err)
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, classifyOperational(OpDial, ogrenet.SchemeUDP, nil, nil, mapped, hintNone)
	}
	conn, ok := raw.(*net.UDPConn)
	if !ok {
		local, remote := raw.LocalAddr(), raw.RemoteAddr()
		_ = raw.Close()
		return nil, classifyOperational(OpDial, ogrenet.SchemeUDP, local, remote, fmt.Errorf("transport: UDP dial returned %T", raw), hintNone)
	}
	remote, ok := conn.RemoteAddr().(*net.UDPAddr)
	if !ok {
		local, rawRemote := conn.LocalAddr(), conn.RemoteAddr()
		_ = conn.Close()
		return nil, classifyOperational(OpDial, ogrenet.SchemeUDP, local, rawRemote, fmt.Errorf("transport: UDP remote returned %T", rawRemote), hintNone)
	}
	p := e.newPacketConn(conn, endpoint, remote, h)
	if err := e.addPacket(p); err != nil {
		local := conn.LocalAddr()
		_ = conn.Close()
		return nil, classifyOperational(OpDial, ogrenet.SchemeUDP, local, remote, err, hintNone)
	}
	p.activity = newActivityClock(e.cfg.timeouts.ConnectionIdle, e.cfg.timeouts.MaxLifetime)
	p.start()
	return p, nil
}

func (e *Engine) newPacketConn(conn *net.UDPConn, endpoint ogrenet.Endpoint, remote *net.UDPAddr, h ogrenet.PacketHandler) *packetConn {
	return &packetConn{
		engine:    e,
		endpoint:  endpoint,
		conn:      conn,
		remote:    remote,
		handler:   h,
		maxPacket: e.cfg.maxDatagramBytes,
		timeouts:  e.cfg.timeouts,
		queue:     make(chan packetOutbound, e.cfg.writeQueue),
		quota:     newByteQuota(e.cfg.maxQueuedBytes),
		gate:      newSendGate(),
		slots:     make(chan struct{}, e.cfg.writeQueue+1),
		drainReq:  make(chan struct{}),
		closing:   make(chan struct{}),
		done:      make(chan struct{}),
	}
}

var _ ogrenet.PacketConn = (*packetConn)(nil)
