package transport

import (
	"context"

	"github.com/qigao/ogrenet"
)

func (e *Engine) ListenPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolMismatch
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()
	return e.listenPacket(ctx, endpoint, normalizePacketHandler(h))
}

func (e *Engine) DialPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolMismatch
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()
	return e.dialPacket(ctx, endpoint, normalizePacketHandler(h))
}
