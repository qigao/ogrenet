package transport

import (
	"context"

	"github.com/qigao/ogrenet"
)

func (e *Engine) Listen(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Listener, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.Validate(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsSession() {
		return nil, ErrProtocolMismatch
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()

	switch endpoint.Scheme {
	case ogrenet.SchemeTCP, ogrenet.SchemeTLS:
		return e.listenStream(ctx, endpoint, normalizeHandler(h))
	case ogrenet.SchemeWS, ogrenet.SchemeWSS:
		return e.listenWebSocket(ctx, endpoint, normalizeHandler(h))
	default:
		return nil, ErrProtocolMismatch
	}
}

func (e *Engine) Dial(ctx context.Context, endpoint ogrenet.Endpoint, h ogrenet.Handler) (ogrenet.Session, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if !endpoint.Scheme.IsSession() {
		return nil, ErrProtocolMismatch
	}
	if err := e.beginOp(); err != nil {
		return nil, err
	}
	defer e.endOp()

	switch endpoint.Scheme {
	case ogrenet.SchemeTCP, ogrenet.SchemeTLS:
		return e.dialStream(ctx, endpoint, normalizeHandler(h))
	case ogrenet.SchemeWS, ogrenet.SchemeWSS:
		return e.dialWebSocket(ctx, endpoint, normalizeHandler(h))
	default:
		return nil, ErrProtocolMismatch
	}
}
