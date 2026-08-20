package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	quicgo "github.com/quic-go/quic-go"
)

const tlsAlertNoApplicationProtocol tls.AlertError = 120

// HTTPAttemptError records one protocol attempt and its failure.
type HTTPAttemptError struct {
	Protocol HTTPProtocol
	Err      error
}

// HTTPFallbackError reports a failed ordered protocol fallback sequence while
// preserving every underlying error for errors.Is / errors.As.
type HTTPFallbackError struct {
	Attempts []HTTPAttemptError
}

func (e *HTTPFallbackError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if len(e.Attempts) == 0 {
		return "client: HTTP protocol fallback failed"
	}
	var b strings.Builder
	b.WriteString("client: HTTP protocol fallback failed")
	for _, attempt := range e.Attempts {
		fmt.Fprintf(&b, "; %s: %v", attempt.Protocol, attempt.Err)
	}
	return b.String()
}

func (e *HTTPFallbackError) Unwrap() []error {
	if e == nil || len(e.Attempts) == 0 {
		return nil
	}
	errs := make([]error, 0, len(e.Attempts))
	for _, attempt := range e.Attempts {
		if attempt.Err != nil {
			errs = append(errs, attempt.Err)
		}
	}
	return errs
}

func newHTTPFallbackError(attempts []HTTPAttemptError) error {
	copied := append([]HTTPAttemptError(nil), attempts...)
	return &HTTPFallbackError{Attempts: copied}
}

type fallbackClass uint8

const (
	fallbackNever fallbackClass = iota
	fallbackPreRequest
	fallbackAmbiguousAfterSend
)

func (t *HTTPClientTransport) roundTrip(req *http.Request) (*http.Response, error) {
	if t == nil || t.closed.Load() {
		return nil, ErrHTTPClientTransportClosed
	}
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", ErrInvalidHTTPClientConfig)
	}

	applicable := make([]protocolTransport, 0, len(t.attempts))
	for _, attempt := range t.attempts {
		if protocolApplies(attempt.protocol, req) {
			applicable = append(applicable, attempt)
		}
	}
	if len(applicable) == 0 {
		return nil, ErrNoApplicableHTTPProtocol
	}

	canReplay := requestCanReplay(req)
	failures := make([]HTTPAttemptError, 0, len(applicable))
	for index, attempt := range applicable {
		attemptReq, err := cloneAttemptRequest(req, attempt.protocol, index)
		if err != nil {
			prepErr := fmt.Errorf("client: prepare %s attempt: %w", attempt.protocol, err)
			if len(failures) == 0 {
				return nil, prepErr
			}
			failures = append(failures, HTTPAttemptError{Protocol: attempt.protocol, Err: prepErr})
			return nil, newHTTPFallbackError(failures)
		}

		resp, err := attempt.rt.RoundTrip(attemptReq)
		if resp != nil {
			return resp, err
		}
		if err == nil {
			err = errors.New("client: protocol transport returned nil response and nil error")
		}
		if cause := context.Cause(req.Context()); cause != nil {
			return nil, cause
		}

		failures = append(failures, HTTPAttemptError{Protocol: attempt.protocol, Err: err})
		last := index == len(applicable)-1
		if last || t.fallback != HTTPFallbackSafeReplay || !canReplay || classifyFallback(err) == fallbackNever {
			if len(failures) == 1 {
				return nil, err
			}
			return nil, newHTTPFallbackError(failures)
		}
	}
	return nil, newHTTPFallbackError(failures)
}

func protocolApplies(protocol HTTPProtocol, req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	scheme := strings.ToLower(req.URL.Scheme)
	switch protocol {
	case HTTP1:
		return scheme == "http" || scheme == "https"
	case HTTP2, HTTP3:
		return scheme == "https"
	default:
		return false
	}
}

func requestCanReplay(req *http.Request) bool {
	if req == nil || !isSafeReplayMethod(req.Method) {
		return false
	}
	if req.Body == nil || req.Body == http.NoBody {
		return true
	}
	return req.GetBody != nil
}

func isSafeReplayMethod(method string) bool {
	if method == "" {
		method = http.MethodGet
	}
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}

func cloneAttemptRequest(original *http.Request, protocol HTTPProtocol, index int) (*http.Request, error) {
	ctx := withHTTPAttempt(original.Context(), protocol, index)
	attempt := original.Clone(ctx)
	if index == 0 || original.Body == nil || original.Body == http.NoBody {
		attempt.Body = original.Body
		return attempt, nil
	}
	if original.GetBody == nil {
		return nil, errors.New("request body is not replayable")
	}
	body, err := original.GetBody()
	if err != nil {
		return nil, err
	}
	attempt.Body = body
	return attempt, nil
}

func classifyFallback(err error) fallbackClass {
	if err == nil {
		return fallbackNever
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fallbackNever
	}
	if errors.Is(err, ErrHTTPClientTransportClosed) || errors.Is(err, ErrHTTP3TransportClosed) {
		return fallbackNever
	}
	if errors.Is(err, errHTTPProtocolUnavailable) {
		return fallbackPreRequest
	}

	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostname) || errors.As(err, &invalid) {
		return fallbackNever
	}

	// TLS alert 120 is the RFC 7301 no_application_protocol alert. It proves
	// that this strict HTTP protocol could not be negotiated before an HTTP
	// request reached the application. Other TLS alerts remain terminal.
	var alert tls.AlertError
	if errors.As(err, &alert) {
		if alert == tlsAlertNoApplicationProtocol {
			return fallbackPreRequest
		}
		return fallbackNever
	}

	var h3err *HTTP3Error
	if errors.As(err, &h3err) {
		if h3err.Kind == HTTP3ErrorProtocol || h3err.Kind == HTTP3ErrorClosed {
			return fallbackNever
		}
		var applicationErr *quicgo.ApplicationError
		if errors.As(err, &applicationErr) {
			return fallbackNever
		}
		var versionErr *quicgo.VersionNegotiationError
		var handshakeErr *quicgo.HandshakeTimeoutError
		if errors.As(err, &versionErr) || errors.As(err, &handshakeErr) {
			return fallbackPreRequest
		}
		var resetErr *quicgo.StatelessResetError
		var idleErr *quicgo.IdleTimeoutError
		var transportErr *quicgo.TransportError
		var streamErr *quicgo.StreamError
		if errors.As(err, &resetErr) || errors.As(err, &idleErr) || errors.As(err, &transportErr) || errors.As(err, &streamErr) {
			return fallbackAmbiguousAfterSend
		}
		return fallbackNever
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return fallbackPreRequest
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		switch opErr.Op {
		case "dial":
			return fallbackPreRequest
		case "read", "write":
			return fallbackAmbiguousAfterSend
		}
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return fallbackAmbiguousAfterSend
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fallbackNever
	}
	return fallbackNever
}
