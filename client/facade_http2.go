package client

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
)

var errHTTPProtocolUnavailable = errors.New("client: selected HTTP protocol is unavailable")

// prepareFacadeHTTP2Transport preserves net/http's HTTP/2 implementation while
// making strict H2 negotiation observable as a typed pre-request outcome.
//
// NewHTTPTransport configures Protocols to H2-only. Calling Clone once freezes
// net/http's protocol wiring on the original transport. We can then broaden the
// ClientHello ALPN list to include HTTP/1.1 so an H1-only peer can complete TLS
// instead of returning an opaque TLS alert. VerifyConnection rejects any
// negotiated protocol other than h2 before HTTP request bytes are written.
//
// This path also applies to TLS established after an HTTP CONNECT proxy because
// it uses TLSClientConfig rather than a direct-only DialTLSContext hook.
func prepareFacadeHTTP2Transport(transport *http.Transport) {
	if transport == nil || transport.TLSClientConfig == nil {
		return
	}

	// Clone initializes the original transport's protocol hooks via the public
	// Clone contract before we deliberately broaden the ALPN ClientHello below.
	_ = transport.Clone()

	tlsCfg := transport.TLSClientConfig.Clone()
	previousVerify := tlsCfg.VerifyConnection
	tlsCfg.NextProtos = []string{"h2", "http/1.1"}
	tlsCfg.VerifyConnection = func(state tls.ConnectionState) error {
		if previousVerify != nil {
			if err := previousVerify(state); err != nil {
				return err
			}
		}
		if state.NegotiatedProtocol != "h2" {
			return fmt.Errorf("%w: wanted h2, negotiated %q", errHTTPProtocolUnavailable, state.NegotiatedProtocol)
		}
		return nil
	}
	transport.TLSClientConfig = tlsCfg
}
