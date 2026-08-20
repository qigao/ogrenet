package quic

import (
	"crypto/tls"
	"time"

	"github.com/qigao/ogrenet/internal/quicpolicy"
	quicgo "github.com/quic-go/quic-go"
)

var (
	ErrALPNRequired   = quicpolicy.ErrALPNRequired
	ErrInvalidTimeout = quicpolicy.ErrInvalidTimeout
	ErrTLSVersion     = quicpolicy.ErrTLSVersion
)

// Config contains the intentionally small client-side QUIC policy surface.
// Zero timeout values use bounded defaults. Datagram support is opt-in.
//
// The implementation caps peer-initiated bidirectional streams and receive
// windows explicitly, disables peer-initiated unidirectional streams until the
// package exposes a stable API for them, and never enables 0-RTT. Active
// connection migration is likewise not exposed by this package.
type Config struct {
	TLSConfig        *tls.Config
	ALPN             string
	HandshakeTimeout time.Duration
	IdleTimeout      time.Duration
	EnableDatagrams  bool
}

func (c Config) build() (*tls.Config, *quicgo.Config, error) {
	return quicpolicy.Build(quicpolicy.Config{
		TLSConfig:             c.TLSConfig,
		ALPN:                  c.ALPN,
		HandshakeTimeout:      c.HandshakeTimeout,
		IdleTimeout:           c.IdleTimeout,
		EnableDatagrams:       c.EnableDatagrams,
		MaxIncomingStreams:    quicpolicy.DefaultMaxIncomingStreams,
		MaxIncomingUniStreams: -1,
	})
}
