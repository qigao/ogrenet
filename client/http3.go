package client

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	quicgo "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/qigao/ogrenet/internal/quicpolicy"
)

const (
	defaultHTTP3MaxResponseHeaderBytes int64 = 1 << 20
	http3MaxIncomingUniStreams         int64 = 16
)

var (
	ErrInvalidHTTP3Config   = errors.New("client: invalid HTTP/3 transport configuration")
	ErrHTTP3TLSVersion      = errors.New("client: HTTP/3 requires TLS 1.3 or newer")
	ErrHTTP3TransportClosed = errors.New("client: HTTP/3 transport is closed")
)

// HTTP3Config configures the HTTP/3-only client transport. Zero-valued
// timeouts and header limits use bounded defaults. Datagram support is opt-in.
type HTTP3Config struct {
	TLSConfig              *tls.Config
	HandshakeTimeout       time.Duration
	IdleTimeout            time.Duration
	MaxResponseHeaderBytes int64
	DisableCompression     bool
	EnableDatagrams        bool
}

type normalizedHTTP3Config struct {
	tlsConfig              *tls.Config
	quicConfig             *quicgo.Config
	maxResponseHeaderBytes int
	disableCompression     bool
	enableDatagrams        bool
}

// HTTP3ErrorKind is a stable classification for HTTP/3 request failures.
type HTTP3ErrorKind uint8

const (
	HTTP3ErrorUnknown HTTP3ErrorKind = iota
	HTTP3ErrorTransport
	HTTP3ErrorProtocol
	HTTP3ErrorClosed
)

// HTTP3Error wraps an HTTP/3 transport or protocol failure while preserving the
// dependency error as its cause.
type HTTP3Error struct {
	Kind  HTTP3ErrorKind
	Cause error
}

func (e *HTTP3Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("client: HTTP/3 %d: %v", e.Kind, e.Cause)
}

func (e *HTTP3Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func normalizeHTTP3Config(cfg HTTP3Config) (normalizedHTTP3Config, error) {
	if cfg.HandshakeTimeout < 0 || cfg.IdleTimeout < 0 || cfg.MaxResponseHeaderBytes < 0 {
		return normalizedHTTP3Config{}, ErrInvalidHTTP3Config
	}
	maxInt := uint64(^uint(0) >> 1)
	if cfg.MaxResponseHeaderBytes > 0 && uint64(cfg.MaxResponseHeaderBytes) > maxInt {
		return normalizedHTTP3Config{}, ErrInvalidHTTP3Config
	}

	tlsCfg, quicCfg, err := quicpolicy.Build(quicpolicy.Config{
		TLSConfig:             cfg.TLSConfig,
		ALPN:                  http3.NextProtoH3,
		HandshakeTimeout:      cfg.HandshakeTimeout,
		IdleTimeout:           cfg.IdleTimeout,
		EnableDatagrams:       cfg.EnableDatagrams,
		MaxIncomingStreams:    -1,
		MaxIncomingUniStreams: http3MaxIncomingUniStreams,
	})
	if err != nil {
		switch {
		case errors.Is(err, quicpolicy.ErrTLSVersion):
			return normalizedHTTP3Config{}, fmt.Errorf("%w: %v", ErrHTTP3TLSVersion, err)
		case errors.Is(err, quicpolicy.ErrInvalidTimeout):
			return normalizedHTTP3Config{}, fmt.Errorf("%w: %v", ErrInvalidHTTP3Config, err)
		default:
			return normalizedHTTP3Config{}, err
		}
	}

	headerBytes := cfg.MaxResponseHeaderBytes
	if headerBytes == 0 {
		headerBytes = defaultHTTP3MaxResponseHeaderBytes
	}
	return normalizedHTTP3Config{
		tlsConfig:              tlsCfg,
		quicConfig:             quicCfg,
		maxResponseHeaderBytes: int(headerBytes),
		disableCompression:     cfg.DisableCompression,
		enableDatagrams:        cfg.EnableDatagrams,
	}, nil
}
