//go:build linux

package transport

import (
	"net"

	"github.com/qigao/ogrenet"
)

type epollPacketCallbackState uint8

const (
	epollPacketCallbackIdle epollPacketCallbackState = iota + 1
	epollPacketCallbackPacketInFlight
	epollPacketCallbackNeedClose
	epollPacketCallbackCloseInFlight
	epollPacketCallbackClosed
)

type epollPacketCallbackTask struct {
	packet *epollPacketConn
	peer   *net.UDPAddr
	data   []byte
	close  bool
}

func (t *epollPacketCallbackTask) runEpollWorkerTask() {
	if t == nil || t.packet == nil || t.packet.handler == nil {
		return
	}
	if t.close {
		t.packet.handler.OnClose(t.packet, t.packet.Err())
		return
	}
	t.packet.handler.OnPacket(t.packet, cloneUDPAddr(t.peer), ogrenet.Packet{Data: t.data})
}

func (t *epollPacketCallbackTask) onEpollWorkerComplete() {
	if t == nil || t.packet == nil {
		return
	}
	t.packet.publishNativePacketCallbackCompletion()
}

func (p *epollPacketConn) initNativePacketReadCallbacks() {
	if p == nil || p.callbackState != 0 {
		return
	}
	p.callbackState = epollPacketCallbackIdle
	size := p.maxPacket
	if size <= 0 {
		size = defaultMaxDatagramBytes
	}
	p.readScratch = make([]byte, size)
}

func (p *epollPacketConn) publishNativePacketCallbackCompletion() {
	if p == nil {
		return
	}
	p.callbackMu.Lock()
	p.callbackCompleted = true
	p.callbackMu.Unlock()
	if p.reactor != nil {
		p.reactor.signal(p)
	}
}

func (p *epollPacketConn) consumeNativePacketCallbackCompletion() {
	if p == nil {
		return
	}
	p.callbackMu.Lock()
	completed := p.callbackCompleted
	p.callbackCompleted = false
	p.callbackMu.Unlock()
	if !completed {
		return
	}
	switch p.callbackState {
	case epollPacketCallbackPacketInFlight:
		p.callbackState = epollPacketCallbackIdle
	case epollPacketCallbackCloseInFlight:
		p.callbackState = epollPacketCallbackClosed
	}
}

func (p *epollPacketConn) driveNativePacketCallbackState(r *epollReactor) {
	if p == nil || r == nil || p.engine == nil || p.engine.callbacks == nil {
		return
	}
	if p.terminalPrepared && p.callbackState == epollPacketCallbackIdle {
		p.callbackState = epollPacketCallbackNeedClose
	}
	if p.callbackState != epollPacketCallbackNeedClose {
		return
	}
	if p.handler == nil {
		p.callbackState = epollPacketCallbackClosed
		return
	}
	if !p.engine.callbacks.tryReserve() {
		r.blockOnWorker(p)
		return
	}
	p.callbackState = epollPacketCallbackCloseInFlight
	p.engine.callbacks.submitReserved(&epollPacketCallbackTask{packet: p, close: true})
}
