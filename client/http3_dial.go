package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"

	quicgo "github.com/quic-go/quic-go"
)

type http3Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
	LookupPort(context.Context, string, string) (int, error)
}

type http3Dialer struct {
	mu        sync.Mutex
	resolver  http3Resolver
	udp       *net.UDPConn
	transport *quicgo.Transport
	closed    bool
	listenUDP func(string, *net.UDPAddr) (*net.UDPConn, error)
	dialQUIC  func(*quicgo.Transport, context.Context, net.Addr, *tls.Config, *quicgo.Config) (*quicgo.Conn, error)
}

func newHTTP3Dialer() *http3Dialer {
	return &http3Dialer{
		resolver:  net.DefaultResolver,
		listenUDP: net.ListenUDP,
		dialQUIC: func(tr *quicgo.Transport, ctx context.Context, addr net.Addr, tlsCfg *tls.Config, qcfg *quicgo.Config) (*quicgo.Conn, error) {
			return tr.Dial(ctx, addr, tlsCfg, qcfg)
		},
	}
}

func (d *http3Dialer) Dial(ctx context.Context, addr string, tlsCfg *tls.Config, qcfg *quicgo.Config) (*quicgo.Conn, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := d.resolver.LookupPort(ctx, "udp", portText)
	if err != nil {
		return nil, err
	}
	ips, err := d.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, errors.New("client: HTTP/3 resolver returned no addresses")
	}
	remote := &net.UDPAddr{IP: ips[0].IP, Port: port, Zone: ips[0].Zone}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, ErrHTTP3TransportClosed
	}
	if d.transport == nil {
		udp, err := d.listenUDP("udp", nil)
		if err != nil {
			d.mu.Unlock()
			return nil, err
		}
		d.udp = udp
		d.transport = &quicgo.Transport{Conn: udp}
	}
	tr := d.transport
	dial := d.dialQUIC
	d.mu.Unlock()

	return dial(tr, ctx, remote, tlsCfg, qcfg)
}

func (d *http3Dialer) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	tr := d.transport
	udp := d.udp
	d.transport = nil
	d.udp = nil
	d.mu.Unlock()

	var first error
	if tr != nil {
		if err := tr.Close(); err != nil {
			first = err
		}
	}
	if udp != nil {
		if err := udp.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
