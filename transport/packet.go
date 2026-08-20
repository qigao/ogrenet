package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

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
	queue     chan packetOutbound
	quota     *byteQuota
	gate      *sendGate
	slots     chan struct{}
	closing   chan struct{}
	done      chan struct{}

	closeOnce sync.Once
	finalOnce sync.Once
	loops     sync.WaitGroup
	errMu     sync.RWMutex
	err       error
}

func (p *packetConn) Protocol() ogrenet.Scheme       { return ogrenet.SchemeUDP }
func (p *packetConn) Endpoint() ogrenet.Endpoint     { return p.endpoint }
func (p *packetConn) LocalAddr() net.Addr            { return p.conn.LocalAddr() }
func (p *packetConn) Done() <-chan struct{}           { return p.done }
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
			return nil, fmt.Errorf("transport: resolve UDP peer: %w", err)
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.gate.enter() {
		return ErrClosed
	}
	defer p.gate.leave()
	if err := p.validatePacket(packet); err != nil {
		return err
	}
	if err := p.acquireSlot(ctx); err != nil {
		return err
	}
	held := true
	defer func() {
		if held {
			p.releaseSlot()
		}
	}()
	if err := p.quota.acquire(ctx, p.closing, len(packet.Data)); err != nil {
		return err
	}
	data := append([]byte(nil), packet.Data...)
	held = false

	ack := make(chan error, 1)
	req := packetOutbound{peer: peer, data: data, ack: ack, bytes: len(data)}
	select {
	case <-ctx.Done():
		p.quota.release(req.bytes)
		p.releaseSlot()
		return ctx.Err()
	case <-p.closing:
		p.quota.release(req.bytes)
		p.releaseSlot()
		return ErrClosed
	case p.queue <- req:
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closing:
		select {
		case err := <-ack:
			return err
		default:
			return ErrClosed
		}
	}
}

func (p *packetConn) trySend(peer *net.UDPAddr, packet ogrenet.Packet) error {
	if !p.gate.enter() {
		return ErrClosed
	}
	defer p.gate.leave()
	if err := p.validatePacket(packet); err != nil {
		return err
	}
	if err := p.tryAcquireSlot(); err != nil {
		return err
	}
	held := true
	defer func() {
		if held {
			p.releaseSlot()
		}
	}()
	if err := p.quota.tryAcquire(len(packet.Data)); err != nil {
		return err
	}
	data := append([]byte(nil), packet.Data...)
	held = false
	req := packetOutbound{peer: peer, data: data, bytes: len(data)}
	select {
	case <-p.closing:
		p.quota.release(req.bytes)
		p.releaseSlot()
		return ErrClosed
	case p.queue <- req:
		return nil
	default:
		p.quota.release(req.bytes)
		p.releaseSlot()
		return ErrWouldBlock
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
	p.loops.Add(2)
	go func() {
		defer p.loops.Done()
		p.writerLoop()
	}()
	go func() {
		defer p.loops.Done()
		p.readerLoop()
	}()
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
	for {
		select {
		case <-p.closing:
			return
		case req := <-p.queue:
			var err error
			if req.peer == nil {
				_, err = p.conn.Write(req.data)
			} else {
				_, err = p.conn.WriteToUDP(req.data, req.peer)
			}
			p.quota.release(req.bytes)
			p.releaseSlot()
			sendErr := err
			if err != nil && p.isClosing() {
				sendErr = ErrClosed
			}
			if req.ack != nil {
				req.ack <- sendErr
			}
			if err != nil {
				p.initiateClose(fmt.Errorf("transport: UDP write: %w", err))
				return
			}
		}
	}
}

func (p *packetConn) failPending(err error) {
	for {
		select {
		case req := <-p.queue:
			p.quota.release(req.bytes)
			p.releaseSlot()
			if req.ack != nil {
				req.ack <- err
			}
		default:
			return
		}
	}
}

func (p *packetConn) readerLoop() {
	buf := make([]byte, 65535)
	for {
		n, peer, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			p.initiateClose(normalizePacketError(err))
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
		cause = normalizePacketError(cause)
		p.errMu.Lock()
		p.err = cause
		p.errMu.Unlock()
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
	addr, err := net.ResolveUDPAddr("udp", endpoint.Address())
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	p := e.newPacketConn(conn, boundEndpoint(endpoint, conn.LocalAddr()), nil, h)
	if err := e.addPacket(p); err != nil {
		_ = conn.Close()
		return nil, err
	}
	p.start()
	go func() {
		select {
		case <-ctx.Done():
			p.initiateClose(ctx.Err())
		case <-p.done:
		}
	}()
	return p, nil
}

func (e *Engine) dialPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	remote, err := net.ResolveUDPAddr("udp", endpoint.Address())
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, remote)
	if err != nil {
		return nil, err
	}
	p := e.newPacketConn(conn, endpoint, remote, h)
	if err := e.addPacket(p); err != nil {
		_ = conn.Close()
		return nil, err
	}
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
		queue:     make(chan packetOutbound, e.cfg.writeQueue),
		quota:     newByteQuota(e.cfg.maxQueuedBytes),
		gate:      newSendGate(),
		slots:     make(chan struct{}, e.cfg.writeQueue+1),
		closing:   make(chan struct{}),
		done:      make(chan struct{}),
	}
}

var _ ogrenet.PacketConn = (*packetConn)(nil)
