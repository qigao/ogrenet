package quicpolicy

import (
	"crypto/tls"
	"errors"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

const (
	DefaultHandshakeTimeout = 5 * time.Second
	DefaultIdleTimeout      = 30 * time.Second

	DefaultMaxIncomingStreams             int64  = 32
	DefaultInitialStreamReceiveWindow     uint64 = 512 << 10
	DefaultMaxStreamReceiveWindow         uint64 = 4 << 20
	DefaultInitialConnectionReceiveWindow uint64 = 1 << 20
	DefaultMaxConnectionReceiveWindow     uint64 = 8 << 20
)

var (
	ErrALPNRequired   = errors.New("quic: ALPN is required")
	ErrInvalidTimeout = errors.New("quic: timeout must not be negative")
	ErrTLSVersion     = errors.New("quic: TLS minimum version must be TLS 1.3 or newer")
)

type Config struct {
	TLSConfig             *tls.Config
	ALPN                  string
	HandshakeTimeout      time.Duration
	IdleTimeout           time.Duration
	EnableDatagrams       bool
	MaxIncomingStreams    int64
	MaxIncomingUniStreams int64
}

func Build(c Config) (*tls.Config, *quicgo.Config, error) {
	if c.ALPN == "" {
		return nil, nil, ErrALPNRequired
	}
	if c.HandshakeTimeout < 0 || c.IdleTimeout < 0 {
		return nil, nil, ErrInvalidTimeout
	}

	tlsCfg := &tls.Config{}
	if c.TLSConfig != nil {
		tlsCfg = c.TLSConfig.Clone()
	}
	if tlsCfg.MinVersion != 0 && tlsCfg.MinVersion < tls.VersionTLS13 {
		return nil, nil, ErrTLSVersion
	}
	if tlsCfg.MaxVersion != 0 && tlsCfg.MaxVersion < tls.VersionTLS13 {
		return nil, nil, ErrTLSVersion
	}
	if tlsCfg.MinVersion == 0 {
		tlsCfg.MinVersion = tls.VersionTLS13
	}
	tlsCfg.NextProtos = []string{c.ALPN}

	handshake := c.HandshakeTimeout
	if handshake == 0 {
		handshake = DefaultHandshakeTimeout
	}
	idle := c.IdleTimeout
	if idle == 0 {
		idle = DefaultIdleTimeout
	}

	return tlsCfg, &quicgo.Config{
		HandshakeIdleTimeout:           handshake,
		MaxIdleTimeout:                 idle,
		InitialStreamReceiveWindow:     DefaultInitialStreamReceiveWindow,
		MaxStreamReceiveWindow:         DefaultMaxStreamReceiveWindow,
		InitialConnectionReceiveWindow: DefaultInitialConnectionReceiveWindow,
		MaxConnectionReceiveWindow:     DefaultMaxConnectionReceiveWindow,
		MaxIncomingStreams:             c.MaxIncomingStreams,
		MaxIncomingUniStreams:          c.MaxIncomingUniStreams,
		Allow0RTT:                      false,
		EnableDatagrams:                c.EnableDatagrams,
	}, nil
}
