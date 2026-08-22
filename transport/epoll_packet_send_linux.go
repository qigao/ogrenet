//go:build linux

package transport

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/qigao/ogrenet"
)

func (p *epollPacketConn) initNativePacketSendState() {
	if p == nil || p.engine == nil || p.queue != nil {
		return
	}
	p.maxPacket = p.engine.cfg.maxDatagramBytes
	p.queue = make(chan packetOutbound, p.engine.cfg.writeQueue)
	p.quota = newByteQuota(p.engine.cfg.maxQueuedBytes)
	p.quota.setParent(p.engine.admission.bytes)
	p.gate = newSendGate()
	p.slots = make(chan struct{}, p.engine.cfg.writeQueue+1)
	p.closing = make(chan struct{})
}

func (p *epollPacketConn) Send(ctx context.Context, packet ogrenet.Packet) error {
	if p.remote == nil {
		return ErrNotConnected
	}
	return p.sendNativePacket(ctx, nil, packet)
}

func (p *epollPacketConn) TrySend(packet ogrenet.Packet) error {
	if p.remote == nil {
		return ErrNotConnected
	}
	return p.trySendNativePacket(nil, packet)
}

func (p *epollPacketConn) SendTo(ctx context.Context, peer net.Addr, packet ogrenet.Packet) error {
	target, err := p.resolveNativePacketPeer(peer)
	if err != nil {
		return err
	}
	return p.sendNativePacket(ctx, target, packet)
}

func (p *epollPacketConn) TrySendTo(peer net.Addr, packet ogrenet.Packet) error {
	target, err := p.resolveNativePacketPeer(peer)
	if err != nil {
		return err
	}
	return p.trySendNativePacket(target, packet)
}

func (p *epollPacketConn) resolveNativePacketPeer(peer net.Addr) (*net.UDPAddr, error) {
	if peer == nil {
		return nil, ErrPeerRequired
	}
	var target *net.UDPAddr
	if udp, ok := peer.(*net.UDPAddr); ok {
		target = cloneUDPAddr(udp)
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

func (p *epollPacketConn) sendNativePacket(ctx context.Context, peer *net.UDPAddr, packet ogrenet.Packet) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if !p.gate.enter() {
		return p.operationalError(OpSend, ErrClosed, peer)
	}
	defer p.leaveNativePacketGate()
	if err := p.validateNativePacket(packet); err != nil {
		return p.operationalError(OpSend, err, peer)
	}
	if err := p.acquireNativePacketSlot(ctx); err != nil {
		return p.nativePacketSendError(ctx, err, peer)
	}
	held := true
	defer func() {
		if held {
			p.releaseNativePacketSlot()
		}
	}()
	if err := p.quota.acquire(ctx, p.closing, len(packet.Data)); err != nil {
		return p.nativePacketSendError(ctx, err, peer)
	}
	data := append([]byte(nil), packet.Data...)
	held = false

	ack := make(chan error, 1)
	req := packetOutbound{peer: peer, data: data, ack: ack, bytes: len(data)}
	select {
	case <-ctx.Done():
		p.quota.release(req.bytes)
		p.releaseNativePacketSlot()
		return context.Cause(ctx)
	case <-p.closing:
		p.quota.release(req.bytes)
		p.releaseNativePacketSlot()
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if terminal := p.Err(); terminal != nil {
			return terminal
		}
		return p.operationalError(OpSend, ErrClosed, peer)
	case p.queue <- req:
		if p.reactor != nil {
			p.reactor.signal(p)
		}
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

func (p *epollPacketConn) trySendNativePacket(peer *net.UDPAddr, packet ogrenet.Packet) error {
	if !p.gate.enter() {
		return p.operationalError(OpSend, ErrClosed, peer)
	}
	defer p.leaveNativePacketGate()
	if err := p.validateNativePacket(packet); err != nil {
		return p.operationalError(OpSend, err, peer)
	}
	if err := p.tryAcquireNativePacketSlot(); err != nil {
		return p.tryNativePacketSendError(err, peer)
	}
	held := true
	defer func() {
		if held {
			p.releaseNativePacketSlot()
		}
	}()
	if err := p.quota.tryAcquire(len(packet.Data)); err != nil {
		return p.tryNativePacketSendError(err, peer)
	}
	data := append([]byte(nil), packet.Data...)
	held = false
	req := packetOutbound{peer: peer, data: data, bytes: len(data)}
	select {
	case <-p.closing:
		p.quota.release(req.bytes)
		p.releaseNativePacketSlot()
		if terminal := p.Err(); terminal != nil {
			return terminal
		}
		return p.operationalError(OpSend, ErrClosed, peer)
	case p.queue <- req:
		if p.reactor != nil {
			p.reactor.signal(p)
		}
		return nil
	default:
		p.quota.release(req.bytes)
		p.releaseNativePacketSlot()
		return p.tryNativePacketSendError(ErrWouldBlock, peer)
	}
}

func (p *epollPacketConn) validateNativePacket(packet ogrenet.Packet) error {
	if len(packet.Data) > p.maxPacket {
		return ErrDatagramTooLarge
	}
	return nil
}

func (p *epollPacketConn) acquireNativePacketSlot(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-p.closing:
		return ErrClosed
	case p.slots <- struct{}{}:
		return nil
	}
}

func (p *epollPacketConn) tryAcquireNativePacketSlot() error {
	select {
	case <-p.closing:
		return ErrClosed
	case p.slots <- struct{}{}:
		return nil
	default:
		return ErrWouldBlock
	}
}

func (p *epollPacketConn) releaseNativePacketSlot() {
	if p != nil && p.slots != nil {
		<-p.slots
	}
}

func (p *epollPacketConn) leaveNativePacketGate() {
	if p == nil || p.gate == nil {
		return
	}
	p.gate.leave()
	if p.closeRequested.Load() && p.reactor != nil {
		p.reactor.signal(p)
	}
}

func (p *epollPacketConn) closeNativePacketAdmission() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		if p.gate != nil {
			p.gate.close()
		}
		if p.closing != nil {
			close(p.closing)
		}
	})
}

func (p *epollPacketConn) nativePacketSendError(ctx context.Context, err error, peer net.Addr) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
	}
	return p.operationalError(OpSend, err, peer)
}

func (p *epollPacketConn) tryNativePacketSendError(err error, peer net.Addr) error {
	opErr := p.operationalError(OpSend, err, peer)
	if errors.Is(err, ErrWouldBlock) {
		if p.stats != nil {
			p.stats.backpressure.Add(1)
		}
		p.observeNativePacket(ogrenet.EventBackpressure, 0, peer, opErr)
	}
	return opErr
}

func (p *epollPacketConn) observeNativePacket(kind ogrenet.EventKind, bytes uint64, peer net.Addr, err error) {
	if p == nil || p.engine == nil || p.engine.observer == nil {
		return
	}
	remote := peer
	if remote == nil {
		remote = p.RemoteAddr()
	}
	p.engine.observer.emit(ogrenet.Event{
		Kind:       kind,
		Resource:   ogrenet.ResourcePacketConn,
		ResourceID: p.id,
		Protocol:   ogrenet.SchemeUDP,
		Local:      p.LocalAddr(),
		Remote:     remote,
		Bytes:      bytes,
		Err:        err,
	})
}

func (p *epollPacketConn) releaseNativePacketPendingOwnership(cause error) {
	if p == nil || p.queue == nil {
		return
	}
	for {
		select {
		case req := <-p.queue:
			if p.quota != nil {
				p.quota.release(req.bytes)
			}
			p.releaseNativePacketSlot()
			if req.ack != nil {
				pendingErr := p.Err()
				if pendingErr == nil {
					pendingErr = p.operationalError(OpSend, cause, req.peer)
				}
				req.ack <- pendingErr
			}
		default:
			return
		}
	}
}
