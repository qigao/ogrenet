package quic

import (
	"context"
	"errors"
	"sync"

	quicgo "github.com/quic-go/quic-go"
)

var (
	ErrNilContext   = errors.New("quic: nil context")
	ErrEmptyAddress = errors.New("quic: empty address")
)

// Conn is a client QUIC connection. It intentionally exposes only the small
// stable surface needed by the first stream-oriented API.
type Conn struct {
	raw       *quicgo.Conn
	closeOnce sync.Once
	closeErr  error
}

// Dial establishes a QUIC connection. The context controls only connection
// establishment; the returned connection has its own lifetime.
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
		return nil, err
	}
	return &Conn{raw: raw}, nil
}

// OpenStream opens one bidirectional QUIC stream.
func (c *Conn) OpenStream(ctx context.Context) (*Stream, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	raw, err := c.raw.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &Stream{raw: raw}, nil
}

// Close closes the QUIC connection with application error code 0. It is
// idempotent at the wrapper boundary.
func (c *Conn) Close() error {
	if c == nil || c.raw == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closeErr = c.raw.CloseWithError(0, "")
	})
	return c.closeErr
}
