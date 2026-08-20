package client

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"
)

const (
	defaultConnectTimeout        = 10 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultResponseHeaderTimeout = 30 * time.Second
	defaultExpectContinueTimeout = time.Second
	defaultIdleConnTimeout       = 90 * time.Second
	defaultKeepAlive             = 30 * time.Second
	defaultMaxIdleConns          = 100
	defaultMaxIdleConnsPerHost   = 16
	defaultMaxConnsPerHost       = 64
	defaultMaxResponseHeader     = 1 << 20
)

// HTTPProtocol identifies a client-side HTTP protocol.
type HTTPProtocol uint8

const (
	HTTP1 HTTPProtocol = iota + 1
	HTTP2
	HTTP3
)

func (p HTTPProtocol) String() string {
	switch p {
	case HTTP1:
		return "http/1.1"
	case HTTP2:
		return "h2"
	case HTTP3:
		return "h3"
	default:
		return "unknown"
	}
}

// ProxyFunc matches net/http.Transport.Proxy. A nil ProxyFunc means direct
// connections only; environment proxy variables are not consulted implicitly.
type ProxyFunc func(*http.Request) (*url.URL, error)

// HTTPConfig configures an HTTP/1.1 and HTTP/2 client transport. Zero-valued
// duration and numeric fields use bounded production defaults. Protocols defaults
// to HTTP/1.1 + HTTP/2 when empty.
type HTTPConfig struct {
	Protocols []HTTPProtocol

	ConnectTimeout        time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	ExpectContinueTimeout time.Duration
	IdleConnTimeout       time.Duration
	KeepAlive             time.Duration

	MaxIdleConns           int
	MaxIdleConnsPerHost    int
	MaxConnsPerHost        int
	MaxResponseHeaderBytes int64

	DisableCompression bool
	TLSConfig          *tls.Config
	Proxy              ProxyFunc
}

var (
	ErrInvalidHTTPConfig   = errors.New("client: invalid HTTP transport configuration")
	ErrInvalidHTTPProtocol = errors.New("client: invalid HTTP protocol")
	ErrHTTPSTLSVersion     = errors.New("client: HTTPS requires TLS 1.3 or newer")
)

// NewHTTPTransport creates a reusable, concurrent-safe HTTP/1.1 / HTTP/2
// transport. The transport never consults environment proxy variables unless the
// caller explicitly supplies http.ProxyFromEnvironment as HTTPConfig.Proxy.
func NewHTTPTransport(cfg HTTPConfig) (*http.Transport, error) {
	normalized, err := normalizeHTTPConfig(cfg)
	if err != nil {
		return nil, err
	}

	tlsCfg, err := normalizeTLSConfig(normalized.TLSConfig)
	if err != nil {
		return nil, err
	}

	dialer := &net.Dialer{
		Timeout:   normalized.ConnectTimeout,
		KeepAlive: normalized.KeepAlive,
	}
	transport := &http.Transport{
		Proxy:                  normalized.Proxy,
		DialContext:            dialer.DialContext,
		TLSClientConfig:        tlsCfg,
		TLSHandshakeTimeout:    normalized.TLSHandshakeTimeout,
		ResponseHeaderTimeout:  normalized.ResponseHeaderTimeout,
		ExpectContinueTimeout:  normalized.ExpectContinueTimeout,
		IdleConnTimeout:        normalized.IdleConnTimeout,
		MaxIdleConns:           normalized.MaxIdleConns,
		MaxIdleConnsPerHost:    normalized.MaxIdleConnsPerHost,
		MaxConnsPerHost:        normalized.MaxConnsPerHost,
		MaxResponseHeaderBytes: normalized.MaxResponseHeaderBytes,
		DisableCompression:     normalized.DisableCompression,
	}
	if err := applyHTTPProtocols(transport, normalized.Protocols); err != nil {
		return nil, err
	}
	return transport, nil
}

// NewHTTPClient creates an http.Client using NewHTTPTransport. It intentionally
// leaves Client.Timeout unset so streaming response bodies are governed by the
// request context rather than an implicit whole-request deadline.
func NewHTTPClient(cfg HTTPConfig) (*http.Client, error) {
	transport, err := NewHTTPTransport(cfg)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

func normalizeHTTPConfig(cfg HTTPConfig) (HTTPConfig, error) {
	if err := validateNonNegativeDurations(cfg); err != nil {
		return HTTPConfig{}, err
	}
	if cfg.MaxIdleConns < 0 || cfg.MaxIdleConnsPerHost < 0 || cfg.MaxConnsPerHost < 0 || cfg.MaxResponseHeaderBytes < 0 {
		return HTTPConfig{}, ErrInvalidHTTPConfig
	}

	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	if cfg.TLSHandshakeTimeout == 0 {
		cfg.TLSHandshakeTimeout = defaultTLSHandshakeTimeout
	}
	if cfg.ResponseHeaderTimeout == 0 {
		cfg.ResponseHeaderTimeout = defaultResponseHeaderTimeout
	}
	if cfg.ExpectContinueTimeout == 0 {
		cfg.ExpectContinueTimeout = defaultExpectContinueTimeout
	}
	if cfg.IdleConnTimeout == 0 {
		cfg.IdleConnTimeout = defaultIdleConnTimeout
	}
	if cfg.KeepAlive == 0 {
		cfg.KeepAlive = defaultKeepAlive
	}
	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = defaultMaxIdleConns
	}
	if cfg.MaxIdleConnsPerHost == 0 {
		cfg.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	}
	if cfg.MaxConnsPerHost == 0 {
		cfg.MaxConnsPerHost = defaultMaxConnsPerHost
	}
	if cfg.MaxResponseHeaderBytes == 0 {
		cfg.MaxResponseHeaderBytes = defaultMaxResponseHeader
	}

	protocols, err := normalizeProtocols(cfg.Protocols)
	if err != nil {
		return HTTPConfig{}, err
	}
	cfg.Protocols = protocols
	return cfg, nil
}

func validateNonNegativeDurations(cfg HTTPConfig) error {
	for _, d := range []time.Duration{
		cfg.ConnectTimeout,
		cfg.TLSHandshakeTimeout,
		cfg.ResponseHeaderTimeout,
		cfg.ExpectContinueTimeout,
		cfg.IdleConnTimeout,
		cfg.KeepAlive,
	} {
		if d < 0 {
			return ErrInvalidHTTPConfig
		}
	}
	return nil
}

func normalizeProtocols(protocols []HTTPProtocol) ([]HTTPProtocol, error) {
	if len(protocols) == 0 {
		return []HTTPProtocol{HTTP1, HTTP2}, nil
	}
	seen := make(map[HTTPProtocol]struct{}, len(protocols))
	out := make([]HTTPProtocol, 0, len(protocols))
	for _, protocol := range protocols {
		switch protocol {
		case HTTP1, HTTP2:
		default:
			return nil, fmt.Errorf("%w: %d", ErrInvalidHTTPProtocol, protocol)
		}
		if _, ok := seen[protocol]; ok {
			continue
		}
		seen[protocol] = struct{}{}
		out = append(out, protocol)
	}
	return out, nil
}

func normalizeTLSConfig(cfg *tls.Config) (*tls.Config, error) {
	var out *tls.Config
	if cfg == nil {
		out = &tls.Config{}
	} else {
		out = cfg.Clone()
	}
	if out.MinVersion == 0 {
		out.MinVersion = tls.VersionTLS13
	}
	if out.MinVersion < tls.VersionTLS13 {
		return nil, ErrHTTPSTLSVersion
	}
	// ALPN is owned by HTTPConfig.Protocols. Do not let a caller-supplied
	// tls.Config silently broaden or narrow the selected HTTP protocols.
	out.NextProtos = nil
	return out, nil
}
