//go:build linux

package transport

import (
	"errors"
	"net"

	"github.com/qigao/ogrenet"
	"golang.org/x/sys/unix"
)

func (p *epollPacketConn) driveNativePacketRead(r *epollReactor) {
	if p == nil || r == nil || p.engine == nil || p.engine.callbacks == nil || !p.readReady || p.callbackState != epollPacketCallbackIdle || p.fd < 0 {
		return
	}
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

	for {
		if opsUsed >= opsBudget || bytesUsed >= byteBudget {
			r.requeue(p)
			return
		}
		if !p.engine.callbacks.tryReserve() {
			r.blockOnWorker(p)
			return
		}

		n, _, _, from, err := unix.Recvmsg(p.fd, p.readScratch, nil, unix.MSG_DONTWAIT|unix.MSG_TRUNC)
		opsUsed++
		if err != nil {
			p.engine.callbacks.releaseReserved()
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				p.readReady = false
				return
			}
			p.failNativePacketRead(r, err, nil)
			return
		}
		bytesUsed += n

		peer := cloneUDPAddr(p.remote)
		if peer == nil {
			peer, err = nativeSockaddrToUDPAddr(from)
			if err != nil {
				p.engine.callbacks.releaseReserved()
				p.failNativePacketRead(r, err, nil)
				return
			}
		}

		if n > p.maxPacket {
			p.engine.callbacks.releaseReserved()
			if p.stats != nil {
				p.stats.droppedDatagrams.Add(1)
			}
			p.observeNativePacket(ogrenet.EventDrop, uint64(n), peer, nil)
			continue
		}

		data := append([]byte(nil), p.readScratch[:n]...)
		if p.stats != nil {
			p.stats.bytesRX.Add(uint64(n))
			p.stats.packetsRX.Add(1)
		}
		p.observeNativePacket(ogrenet.EventRead, uint64(n), peer, nil)
		p.callbackState = epollPacketCallbackPacketInFlight
		p.engine.callbacks.submitReserved(&epollPacketCallbackTask{
			packet: p,
			peer:   peer,
			data:   data,
		})
		return
	}
}

func (p *epollPacketConn) failNativePacketRead(r *epollReactor, cause error, peer net.Addr) {
	if p == nil || r == nil || p.state == epollPacketClosed {
		return
	}
	err := p.operationalError(OpReceive, cause, peer)
	p.setTerminalError(err)
	p.closeNativePacketAdmission()
	p.closeRequested.Store(true)
	p.state = epollPacketTerminal
	p.releaseNativePacketWriteOwnership(err)
	p.finalizeReactor(r)
}
