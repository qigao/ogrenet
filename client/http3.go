package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
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

// HTTP3Transport is a reusable, concurrent-safe HTTP/3-only RoundTripper.
type HTTP3Transport struct {
	raw       *http3.Transport
	dialer    *http3Dialer
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

var (
	_ http.RoundTripper = (*HTTP3Transport)(nil)
	_ io.Closer         = (*HTTP3Transport)(nil)
)

// NewHTTP3Transport creates a reusable HTTP/3-only transport. It never falls
// back to HTTP/2 or HTTP/1.1 and uses a non-early QUIC dial path.
func NewHTTP3Transport(cfg HTTP3Config) (*HTTP3Transport, error) {
	n, err := normalizeHTTP3Config(cfg)
	if err != nil {
		return nil, err
	}
	dialer := newHTTP3Dialer()
	raw := &http3.Transport{
		TLSClientConfig:        n.tlsConfig,
		QUICConfig:             n.quicConfig,
		EnableDatagrams:        n.enableDatagrams,
		MaxResponseHeaderBytes: n.maxResponseHeaderBytes,
		DisableCompression:     n.disableCompression,
	}
	raw.Dial = dialer.Dial
	return &HTTP3Transport{raw: raw, dialer: dialer}, nil
}

// NewHTTP3Client creates an http.Client using NewHTTP3Transport. It leaves the
// whole-request Client.Timeout unset so request contexts govern streaming and cancellation.
func NewHTTP3Client(cfg HTTP3Config) (*http.Client, error) {
	tr, err := NewHTTP3Transport(cfg)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: tr}, nil
}

// RoundTrip performs one HTTP/3-only request.
func (t *HTTP3Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.closed.Load() {
		return nil, &HTTP3Error{Kind: HTTP3ErrorClosed, Cause: ErrHTTP3TransportClosed}
	}
	resp, err := t.raw.RoundTrip(req)
	if err != nil {
		return nil, mapHTTP3Error(err)
	}
	return resp, nil
}

// Close closes pooled HTTP/3 connections and the owned QUIC/UDP dial resources.
func (t *HTTP3Transport) Close() error {
	if t == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		if t.raw != nil {
			t.closeErr = t.raw.Close()
		}
		if t.dialer != nil {
			if err := t.dialer.Close(); err != nil && t.closeErr == nil {
				t.closeErr = err
			}
		}
	})
	return t.closeErr
}

// CloseIdleConnections closes currently idle pooled HTTP/3 connections without
// closing active requests or the shared UDP dial transport.
func (t *HTTP3Transport) CloseIdleConnections() {
	if t == nil || t.raw == nil {
		return
	}
	t.raw.CloseIdleConnections()
}

func mapHTTP3Error(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, http3.ErrTransportClosed) || errors.Is(err, quicgo.ErrTransportClosed) || errors.Is(err, ErrHTTP3TransportClosed) {
		return &HTTP3Error{Kind: HTTP3ErrorClosed, Cause: errors.Join(ErrHTTP3TransportClosed, err)}
	}
	var h3err *http3.Error
	if errors.As(err, &h3err) {
		return &HTTP3Error{Kind: HTTP3ErrorProtocol, Cause: err}
	}
	var transportErr *quicgo.TransportError
	var appErr *quicgo.ApplicationError
	var resetErr *quicgo.StatelessResetError
	var versionErr *quicgo.VersionNegotiationError
	var idleErr *quicgo.IdleTimeoutError
	var handshakeErr *quicgo.HandshakeTimeoutError
	if errors.As(err, &transportErr) || errors.As(err, &appErr) || errors.As(err, &resetErr) || errors.As(err, &versionErr) || errors.As(err, &idleErr) || errors.As(err, &handshakeErr) {
		return &HTTP3Error{Kind: HTTP3ErrorTransport, Cause: err}
	}
	return err
}
