package transport

import (
	"context"
	"net"

	"github.com/qigao/ogrenet"
)

func (p *packetConn) operationalError(op Op, cause error, remote net.Addr) error {
	if remote == nil {
		remote = p.RemoteAddr()
	}
	return classifyOperational(op, ogrenet.SchemeUDP, p.LocalAddr(), remote, cause, hintNone)
}

func (p *packetConn) sendError(ctx context.Context, err error, remote net.Addr) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
	}
	return p.operationalError(OpSend, err, remote)
}
