//go:build linux

package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
	"golang.org/x/sys/unix"
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
		if p.testAfterPacketQueueTransfer != nil {
			p.testAfterPacketQueueTransfer(req)
		}
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
		if p.testAfterPacketQueueTransfer != nil {
			p.testAfterPacketQueueTransfer(req)
		}
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
	if p.reactor != nil && (p.closeRequested.Load() || isClosedSignal(p.gate.done())) {
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

func (p *epollPacketConn) currentDeadlineGeneration(kind epollDeadlineKind) uint64 {
	if p == nil || kind != epollDeadlineWrite {
		return 0
	}
	return p.writeGen
}

func (p *epollPacketConn) onReactorDeadline(r *epollReactor, kind epollDeadlineKind, generation uint64) {
	if kind == epollDeadlineWrite {
		p.onNativePacketWriteDeadline(r, generation)
	}
}

func (p *epollPacketConn) onNativePacketWriteDeadline(r *epollReactor, generation uint64) {
	if p == nil || r == nil || p.state != epollPacketActive || !p.writeActive || generation != p.writeGen {
		return
	}
	timeout := &TimeoutError{Kind: TimeoutWrite, Cause: context.DeadlineExceeded}
	p.failNativePacketWrite(r, p.operationalError(OpWrite, timeout, p.writeCurrent.peer))
}

func (p *epollPacketConn) driveNativePacketWrite(r *epollReactor) {
	if p == nil || r == nil || p.state != epollPacketActive || p.fd < 0 || p.writeBlocked {
		return
	}
	defer p.finalizeNativePacketEngineDrain(r)

	opsBudget := r.cfg.ioBudgetOps
	if opsBudget <= 0 {
		opsBudget = 1
	}
	byteBudget := r.cfg.ioBudgetBytes
	if byteBudget <= 0 {
		byteBudget = 1
	}
	opsUsed := 0
	bytesUsed := 0

	for opsUsed < opsBudget && bytesUsed < byteBudget {
		if !p.writeActive {
			select {
			case p.writeCurrent = <-p.queue:
				p.writeActive = true
				p.writeGen++
				if p.engine != nil {
					if timeout := p.engine.cfg.timeouts.Write; timeout > 0 {
						r.scheduleDeadline(p.id, epollDeadlineWrite, p.writeGen, time.Now().Add(timeout))
					}
				}
			default:
				return
			}
		}

		req := p.writeCurrent
		n, err := p.writeNativePacketDatagram(req)
		opsUsed++
		if err == nil && n == len(req.data) {
			bytesUsed += n
			if p.writeInterested && !p.disableNativePacketWriteInterest(r) {
				return
			}
			p.completeNativePacketWrite()
			p.noteNativePacketWriteProgress(r)
			continue
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			p.writeBlocked = true
			p.enableNativePacketWriteInterest(r)
			return
		}
		if err == nil && n != len(req.data) {
			err = io.ErrShortWrite
		}
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		p.failNativePacketWrite(r, p.operationalError(OpWrite, err, req.peer))
		return
	}
	if p.writeActive || len(p.queue) != 0 {
		r.requeue(p)
	}
}

func (p *epollPacketConn) finalizeNativePacketEngineDrain(r *epollReactor) {
	if p == nil || r == nil || p.state != epollPacketActive || p.closeRequested.Load() || p.gate == nil {
		return
	}
	if !isClosedSignal(p.gate.done()) || p.writeActive || len(p.queue) != 0 {
		return
	}
	p.finalizeReactor(r)
}

func (p *epollPacketConn) writeNativePacketDatagram(req packetOutbound) (int, error) {
	if p.testWriteDatagram != nil {
		return p.testWriteDatagram(req)
	}
	var to unix.Sockaddr
	if req.peer != nil {
		sa, _, err := nativeUDPAddrToSockaddr(req.peer)
		if err != nil {
			return 0, err
		}
		to = sa
	}
	return unix.SendmsgN(p.fd, req.data, nil, to, 0)
}

func (p *epollPacketConn) enableNativePacketWriteInterest(r *epollReactor) {
	if p == nil || r == nil || p.writeInterested || !p.registered || p.fd < 0 {
		return
	}
	if err := r.poller.Mod(p.fd, epoll.Readable|epoll.Writable|epoll.Error|epoll.EdgeTriggered, p.id); err != nil {
		p.failNativePacketWrite(r, p.operationalError(OpWrite, err, p.writeCurrent.peer))
		return
	}
	p.writeInterested = true
}

func (p *epollPacketConn) disableNativePacketWriteInterest(r *epollReactor) bool {
	if p == nil || r == nil || !p.writeInterested || !p.registered || p.fd < 0 {
		return true
	}
	if err := r.poller.Mod(p.fd, epoll.Readable|epoll.Error|epoll.EdgeTriggered, p.id); err != nil {
		p.failNativePacketWrite(r, p.operationalError(OpWrite, err, p.writeCurrent.peer))
		return false
	}
	p.writeInterested = false
	p.writeBlocked = false
	return true
}

func (p *epollPacketConn) completeNativePacketWrite() {
	req := p.writeCurrent
	p.writeCurrent = packetOutbound{}
	p.writeActive = false
	p.writeBlocked = false
	p.writeGen++
	if p.quota != nil {
		p.quota.release(req.bytes)
	}
	p.releaseNativePacketSlot()
	if p.stats != nil {
		p.stats.bytesTX.Add(uint64(len(req.data)))
		p.stats.packetsTX.Add(1)
	}
	p.observeNativePacket(ogrenet.EventWrite, uint64(len(req.data)), req.peer, nil)
	if req.ack != nil {
		req.ack <- nil
	}
}

func (p *epollPacketConn) failNativePacketWrite(r *epollReactor, err error) {
	if p == nil || r == nil || p.state == epollPacketClosed {
		return
	}
	p.setTerminalError(err)
	p.closeNativePacketAdmission()
	p.closeRequested.Store(true)
	p.state = epollPacketTerminal
	p.releaseNativePacketWriteOwnership(err)
	p.finalizeReactor(r)
}

func (p *epollPacketConn) releaseNativePacketWriteOwnership(cause error) {
	if p == nil || p.queue == nil {
		return
	}
	if p.writeActive {
		req := p.writeCurrent
		p.writeCurrent = packetOutbound{}
		p.writeActive = false
		p.writeBlocked = false
		p.writeInterested = false
		p.writeGen++
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
	}
	p.releaseNativePacketPendingOwnership(cause)
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
