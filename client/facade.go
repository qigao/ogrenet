package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

var (
	ErrInvalidHTTPClientConfig   = errors.New("client: invalid HTTP client configuration")
	ErrHTTPClientTransportClosed = errors.New("client: HTTP client transport is closed")
	ErrNoApplicableHTTPProtocol  = errors.New("client: no configured HTTP protocol applies to request")
)

// HTTPFallbackPolicy controls whether the facade may advance to a later
// configured protocol after an eligible transport failure.
type HTTPFallbackPolicy uint8

const (
	HTTPFallbackDisabled HTTPFallbackPolicy = iota
	HTTPFallbackSafeReplay
)

// HTTPClientConfig configures the explicit ordered HTTP protocol facade.
// Protocols is required and is the only protocol-ordering source in facade mode.
type HTTPClientConfig struct {
	Protocols []HTTPProtocol
	HTTP      HTTPConfig
	HTTP3     HTTP3Config
	Fallback  HTTPFallbackPolicy
}

// HTTPAttemptInfo identifies one protocol attempt made by HTTPClientTransport.
type HTTPAttemptInfo struct {
	Protocol HTTPProtocol
	Index    int
}

type httpAttemptContextKey struct{}

// HTTPAttemptFromContext returns facade attempt metadata attached to a request
// context. It returns false for requests not created by HTTPClientTransport.
func HTTPAttemptFromContext(ctx context.Context) (HTTPAttemptInfo, bool) {
	if ctx == nil {
		return HTTPAttemptInfo{}, false
	}
	info, ok := ctx.Value(httpAttemptContextKey{}).(HTTPAttemptInfo)
	return info, ok
}

func withHTTPAttempt(ctx context.Context, protocol HTTPProtocol, index int) context.Context {
	return context.WithValue(ctx, httpAttemptContextKey{}, HTTPAttemptInfo{Protocol: protocol, Index: index})
}

type protocolTransport struct {
	protocol HTTPProtocol
	rt       http.RoundTripper
}

// HTTPClientTransport composes strict, independently pooled protocol transports
// in the explicit order supplied by HTTPClientConfig.Protocols.
type HTTPClientTransport struct {
	attempts []protocolTransport
	fallback HTTPFallbackPolicy

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

var (
	_ http.RoundTripper = (*HTTPClientTransport)(nil)
	_ io.Closer         = (*HTTPClientTransport)(nil)
)

// NewHTTPRoundTripper creates an explicit ordered multi-protocol HTTP transport.
func NewHTTPRoundTripper(cfg HTTPClientConfig) (*HTTPClientTransport, error) {
	protocols, err := validateHTTPClientConfig(cfg)
	if err != nil {
		return nil, err
	}

	transport := &HTTPClientTransport{
		attempts: make([]protocolTransport, 0, len(protocols)),
		fallback: cfg.Fallback,
	}
	for _, protocol := range protocols {
		rt, err := buildProtocolTransport(protocol, cfg)
		if err != nil {
			closeProtocolTransports(transport.attempts)
			return nil, err
		}
		transport.attempts = append(transport.attempts, protocolTransport{protocol: protocol, rt: rt})
	}
	return transport, nil
}

// NewClient creates an http.Client using the explicit protocol facade. It leaves
// Client.Timeout unset so request contexts govern streaming and cancellation.
func NewClient(cfg HTTPClientConfig) (*http.Client, error) {
	transport, err := NewHTTPRoundTripper(cfg)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

func validateHTTPClientConfig(cfg HTTPClientConfig) ([]HTTPProtocol, error) {
	if len(cfg.Protocols) == 0 || len(cfg.HTTP.Protocols) != 0 {
		return nil, ErrInvalidHTTPClientConfig
	}
	if cfg.Fallback != HTTPFallbackDisabled && cfg.Fallback != HTTPFallbackSafeReplay {
		return nil, ErrInvalidHTTPClientConfig
	}

	protocols := append([]HTTPProtocol(nil), cfg.Protocols...)
	seen := make(map[HTTPProtocol]struct{}, len(protocols))
	hasHTTP3 := false
	for _, protocol := range protocols {
		switch protocol {
		case HTTP1, HTTP2:
		case HTTP3:
			hasHTTP3 = true
		default:
			return nil, fmt.Errorf("%w: protocol %d", ErrInvalidHTTPClientConfig, protocol)
		}
		if _, exists := seen[protocol]; exists {
			return nil, fmt.Errorf("%w: duplicate protocol %s", ErrInvalidHTTPClientConfig, protocol)
		}
		seen[protocol] = struct{}{}
	}
	if hasHTTP3 && cfg.HTTP.Proxy != nil {
		return nil, fmt.Errorf("%w: HTTP/3 cannot be combined with HTTP proxy configuration", ErrInvalidHTTPClientConfig)
	}
	return protocols, nil
}

func buildProtocolTransport(protocol HTTPProtocol, cfg HTTPClientConfig) (http.RoundTripper, error) {
	switch protocol {
	case HTTP1:
		httpCfg := cfg.HTTP
		httpCfg.Protocols = []HTTPProtocol{HTTP1}
		return NewHTTPTransport(httpCfg)
	case HTTP2:
		httpCfg := cfg.HTTP
		httpCfg.Protocols = []HTTPProtocol{HTTP2}
		transport, err := NewHTTPTransport(httpCfg)
		if err != nil {
			return nil, err
		}
		prepareFacadeHTTP2Transport(transport)
		return transport, nil
	case HTTP3:
		return NewHTTP3Transport(cfg.HTTP3)
	default:
		return nil, fmt.Errorf("%w: protocol %d", ErrInvalidHTTPClientConfig, protocol)
	}
}

// RoundTrip executes the explicit protocol order and applies the configured
// safe-replay fallback policy.
func (t *HTTPClientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.roundTrip(req)
}

// CloseIdleConnections closes idle connections in every owned protocol slot.
// Active requests remain governed by their request contexts.
func (t *HTTPClientTransport) CloseIdleConnections() {
	if t == nil {
		return
	}
	for _, attempt := range t.attempts {
		if closer, ok := attempt.rt.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	}
}

// Close prevents new requests, closes H3-owned resources, and closes idle H1/H2
// connections. It is safe to call multiple times.
func (t *HTTPClientTransport) Close() error {
	if t == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		t.closeErr = closeProtocolTransports(t.attempts)
	})
	return t.closeErr
}

func closeProtocolTransports(attempts []protocolTransport) error {
	var errs []error
	for _, attempt := range attempts {
		if closer, ok := attempt.rt.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
		if closer, ok := attempt.rt.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", attempt.protocol, err))
			}
		}
	}
	return errors.Join(errs...)
}
