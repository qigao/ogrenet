package quic

import (
	"crypto/tls"
	"errors"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

const (
	defaultHandshakeTimeout = 5 * time.Second
	defaultIdleTimeout      = 30 * time.Second

	defaultMaxIncomingStreams             = 32
	defaultInitialStreamReceiveWindow     = 512 << 10
	defaultMaxStreamReceiveWindow         = 4 << 20
	defaultInitialConnectionReceiveWindow = 1 << 20
	defaultMaxConnectionReceiveWindow     = 8 << 20
)

var (
	ErrALPNRequired   = errors.New("quic: ALPN is required")
	ErrInvalidTimeout = errors.New("quic: timeout must not be negative")
	ErrTLSVersion     = errors.New("quic: TLS minimum version must be TLS 1.3 or newer")
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
	if c.ALPN == "" {
		return nil, nil, ErrALPNRequired
	}
	if c.HandshakeTimeout < 0 || c.IdleTimeout < 0 {
		return nil, nil, ErrInvalidTimeout
	}

	tlsConfig := &tls.Config{}
	if c.TLSConfig != nil {
		tlsConfig = c.TLSConfig.Clone()
	}
	if tlsConfig.MinVersion != 0 && tlsConfig.MinVersion < tls.VersionTLS13 {
		return nil, nil, ErrTLSVersion
	}
	if tlsConfig.MaxVersion != 0 && tlsConfig.MaxVersion < tls.VersionTLS13 {
		return nil, nil, ErrTLSVersion
	}
	if tlsConfig.MinVersion == 0 {
		tlsConfig.MinVersion = tls.VersionTLS13
	}
	tlsConfig.NextProtos = []string{c.ALPN}

	handshakeTimeout := c.HandshakeTimeout
	if handshakeTimeout == 0 {
		handshakeTimeout = defaultHandshakeTimeout
	}
	idleTimeout := c.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultIdleTimeout
	}

	return tlsConfig, &quicgo.Config{
		HandshakeIdleTimeout:           handshakeTimeout,
		MaxIdleTimeout:                 idleTimeout,
		InitialStreamReceiveWindow:     defaultInitialStreamReceiveWindow,
		MaxStreamReceiveWindow:         defaultMaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: defaultInitialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     defaultMaxConnectionReceiveWindow,
		MaxIncomingStreams:             defaultMaxIncomingStreams,
		MaxIncomingUniStreams:          -1,
		Allow0RTT:                      false,
		EnableDatagrams:                c.EnableDatagrams,
	}, nil
}
