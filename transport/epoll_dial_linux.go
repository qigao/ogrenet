//go:build linux

package transport

import (
	"context"
	"net"

	"github.com/qigao/ogrenet"
)

func resolveNativeDialTCP(ctx context.Context, endpoint ogrenet.Endpoint, resolver nativeIPResolver) ([]*net.TCPAddr, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if endpoint.Scheme != ogrenet.SchemeTCP {
		return nil, ErrProtocolMismatch
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if ip := net.ParseIP(endpoint.Host); ip != nil {
		return []*net.TCPAddr{{IP: append(net.IP(nil), ip...), Port: int(endpoint.Port)}}, nil
	}
	if resolver == nil {
		resolver = netNativeIPResolver{}
	}
	addrs, err := resolver.LookupIPAddr(ctx, endpoint.Host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errNativeNoResolvedAddress
	}
	out := make([]*net.TCPAddr, 0, len(addrs))
	for _, addr := range addrs {
		if addr.IP == nil {
			continue
		}
		out = append(out, &net.TCPAddr{
			IP:   append(net.IP(nil), addr.IP...),
			Port: int(endpoint.Port),
			Zone: addr.Zone,
		})
	}
	if len(out) == 0 {
		return nil, errNativeNoResolvedAddress
	}
	return out, nil
}

func (e *epollEngine) dialNativeTCP(ctx context.Context, endpoint ogrenet.Endpoint, handler ogrenet.Handler, resolver nativeIPResolver) (*epollSession, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := endpoint.ValidateDial(); err != nil {
		return nil, err
	}
	if endpoint.Scheme != ogrenet.SchemeTCP {
		return nil, ErrProtocolUnsupported
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
	if _, err := resolveNativeDialTCP(dctx, endpoint, resolver); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		mapped := mapOperationTimeout(ctx, dctx, TimeoutConnect, err)
		return nil, classifyOperational(OpDial, ogrenet.SchemeTCP, nil, nil, mapped, hintNone)
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}

	// Reactor-owned socket/connect progress is introduced by the next TDD step.
	return nil, ErrProtocolUnsupported
}
