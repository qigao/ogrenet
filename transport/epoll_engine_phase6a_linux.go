//go:build linux

package transport

import (
	"context"
	"sync"

	"github.com/qigao/ogrenet"
)

type epollEngine struct {
	cfg      config
	epollCfg resolvedEpollConfig
	done     chan struct{}
	doneOnce sync.Once
}

var _ ogrenet.Engine = (*epollEngine)(nil)

func (e *epollEngine) Listen(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Listener, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsSession() {
		return nil, ErrProtocolMismatch
	}
	return nil, ErrProtocolUnsupported
}

func (e *epollEngine) Dial(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Session, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsSession() {
		return nil, ErrProtocolMismatch
	}
	return nil, ErrProtocolUnsupported
}

func (e *epollEngine) ListenPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsPacket() {
		return nil, ErrProtocolMismatch
	}
	return nil, ErrProtocolUnsupported
}

func (e *epollEngine) DialPacket(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.PacketHandler) (ogrenet.PacketConn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsPacket() {
		return nil, ErrProtocolMismatch
	}
	return nil, ErrProtocolUnsupported
}

func (e *epollEngine) Stats() ogrenet.EngineStats {
	return ogrenet.EngineStats{}
}

func (e *epollEngine) Done() <-chan struct{} {
	return e.done
}

func (e *epollEngine) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	_ = e.Close()
	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (e *epollEngine) Close() error {
	e.doneOnce.Do(func() { close(e.done) })
	return nil
}
