package quic

import (
	"context"
	"errors"
	"net"
	"sync"

	quicgo "github.com/quic-go/quic-go"
)

var (
	ErrNilContext   = errors.New("quic: nil context")
	ErrEmptyAddress = errors.New("quic: empty address")
)

// Conn is a client QUIC connection. QUIC streams and datagrams are exposed as
// native concepts instead of being coerced into transport.Session or PacketConn.
type Conn struct {
	raw       *quicgo.Conn
	datagrams bool
	closeOnce sync.Once
	closeErr  error
}

// Dial establishes a fully handshaken QUIC connection. The context controls
// connection establishment only; the returned connection has its own lifetime.
// DialAddr (not DialAddrEarly) is deliberately used, so 0-RTT is not enabled.
func Dial(ctx context.Context, address string, cfg Config) (*Conn, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if address == "" {
		return nil, ErrEmptyAddress
	}
	tlsConfig, quicConfig, err := cfg.build()
	if err != nil {
		return nil, err
	}
	raw, err := quicgo.DialAddr(ctx, address, tlsConfig, quicConfig)
	if err != nil {
		return nil, wrapError(OpDial, err)
	}
	return &Conn{raw: raw, datagrams: cfg.EnableDatagrams}, nil
}

// LocalAddr returns the local UDP endpoint used by the QUIC connection.
func (c *Conn) LocalAddr() net.Addr { return c.raw.LocalAddr() }

// RemoteAddr returns the current remote QUIC endpoint.
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }

// OpenStream opens one locally initiated bidirectional QUIC stream.
func (c *Conn) OpenStream(ctx context.Context) (*Stream, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	raw, err := c.raw.OpenStreamSync(ctx)
	if err != nil {
		return nil, wrapError(OpOpenStream, err)
	}
	return &Stream{raw: raw}, nil
}

// AcceptStream waits for the next peer-initiated bidirectional stream.
func (c *Conn) AcceptStream(ctx context.Context) (*Stream, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	raw, err := c.raw.AcceptStream(ctx)
	if err != nil {
		return nil, wrapError(OpAcceptStream, err)
	}
	return &Stream{raw: raw}, nil
}

// SendDatagram sends one unreliable RFC 9221 QUIC datagram. Datagram support
// must be enabled explicitly in Config and negotiated by the peer.
func (c *Conn) SendDatagram(payload []byte) error {
	if !c.datagrams {
		return ErrDatagramsDisabled
	}
	return wrapError(OpSendDatagram, c.raw.SendDatagram(payload))
}

// ReceiveDatagram waits for one unreliable RFC 9221 QUIC datagram.
func (c *Conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if !c.datagrams {
		return nil, ErrDatagramsDisabled
	}
	payload, err := c.raw.ReceiveDatagram(ctx)
	if err != nil {
		return nil, wrapError(OpReceiveDatagram, err)
	}
	return payload, nil
}

// Done is closed when the underlying QUIC connection terminates. The
// cancellation cause is exposed through Err.
func (c *Conn) Done() <-chan struct{} { return c.raw.Context().Done() }

// Err returns nil while the connection is alive. Once Done is closed, it
// returns the stable terminal cause classified by ErrorKind.
func (c *Conn) Err() error {
	ctx := c.raw.Context()
	select {
	case <-ctx.Done():
		cause := context.Cause(ctx)
		if cause == nil {
			cause = ctx.Err()
		}
		return wrapError(OpClose, cause)
	default:
		return nil
	}
}

// CloseWithError closes the QUIC connection with an application-defined error
// code and reason. Only the first local close request is sent.
func (c *Conn) CloseWithError(code uint64, reason string) error {
	if c == nil || c.raw == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closeErr = wrapError(OpClose, c.raw.CloseWithError(quicgo.ApplicationErrorCode(code), reason))
	})
	return c.closeErr
}

// Close performs a normal local close using application error code 0.
func (c *Conn) Close() error { return c.CloseWithError(0, "") }
