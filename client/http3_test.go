package client

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

func TestHTTP3ConfigDefaultsAndPolicy(t *testing.T) {
	got, err := normalizeHTTP3Config(HTTP3Config{})
	if err != nil {
		t.Fatal(err)
	}
	if got.tlsConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("TLS min = %d", got.tlsConfig.MinVersion)
	}
	if len(got.tlsConfig.NextProtos) != 1 || got.tlsConfig.NextProtos[0] != http3.NextProtoH3 {
		t.Fatalf("ALPN = %v", got.tlsConfig.NextProtos)
	}
	if got.quicConfig.MaxIncomingStreams != -1 {
		t.Fatalf("peer bidi = %d", got.quicConfig.MaxIncomingStreams)
	}
	if got.quicConfig.MaxIncomingUniStreams != 16 {
		t.Fatalf("peer uni = %d", got.quicConfig.MaxIncomingUniStreams)
	}
	if got.quicConfig.EnableDatagrams || got.quicConfig.Allow0RTT {
		t.Fatalf("datagrams=%v Allow0RTT=%v", got.quicConfig.EnableDatagrams, got.quicConfig.Allow0RTT)
	}
	if got.maxResponseHeaderBytes != 1<<20 {
		t.Fatalf("header bound = %d", got.maxResponseHeaderBytes)
	}
}

func TestHTTP3ConfigValidation(t *testing.T) {
	cases := []struct {
		cfg  HTTP3Config
		want error
	}{
		{HTTP3Config{HandshakeTimeout: -time.Second}, ErrInvalidHTTP3Config},
		{HTTP3Config{IdleTimeout: -time.Second}, ErrInvalidHTTP3Config},
		{HTTP3Config{MaxResponseHeaderBytes: -1}, ErrInvalidHTTP3Config},
		{HTTP3Config{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}, ErrHTTP3TLSVersion},
		{HTTP3Config{TLSConfig: &tls.Config{MaxVersion: tls.VersionTLS12}}, ErrHTTP3TLSVersion},
	}
	for _, tc := range cases {
		if _, err := normalizeHTTP3Config(tc.cfg); !errors.Is(err, tc.want) {
			t.Fatalf("normalize(%+v)=%v, want %v", tc.cfg, err, tc.want)
		}
	}
}

func TestHTTP3DatagramPolicy(t *testing.T) {
	got, err := normalizeHTTP3Config(HTTP3Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	if !got.enableDatagrams || !got.quicConfig.EnableDatagrams {
		t.Fatalf("H3=%v QUIC=%v", got.enableDatagrams, got.quicConfig.EnableDatagrams)
	}
}

func TestHTTP3TLSConfigCloned(t *testing.T) {
	original := &tls.Config{NextProtos: []string{"caller"}}
	got, err := normalizeHTTP3Config(HTTP3Config{TLSConfig: original})
	if err != nil {
		t.Fatal(err)
	}
	if got.tlsConfig == original {
		t.Fatal("TLS config was not cloned")
	}
	if len(original.NextProtos) != 1 || original.NextProtos[0] != "caller" {
		t.Fatalf("caller TLS mutated: %v", original.NextProtos)
	}
}

func TestHTTP3MaxResponseHeaderBytesFitsInt(t *testing.T) {
	maxInt := uint64(^uint(0) >> 1)
	if maxInt < uint64(^uint32(0)>>1) {
		t.Fatalf("unexpected int width")
	}
	if maxInt < uint64(^uint64(0)>>1) {
		cfg := HTTP3Config{MaxResponseHeaderBytes: int64(maxInt + 1)}
		if _, err := normalizeHTTP3Config(cfg); !errors.Is(err, ErrInvalidHTTP3Config) {
			t.Fatalf("overflow = %v", err)
		}
	}
}

func TestHTTP3TransportCloseIsIdempotent(t *testing.T) {
	tr, err := NewHTTP3Transport(HTTP3Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, "https://127.0.0.1/", nil)
	_, err = tr.RoundTrip(req)
	if !errors.Is(err, ErrHTTP3TransportClosed) {
		t.Fatalf("after close: %v", err)
	}
}

func TestHTTP3ClientHasNoWholeRequestTimeout(t *testing.T) {
	c, err := NewHTTP3Client(HTTP3Config{})
	if err != nil {
		t.Fatal(err)
	}
	if c.Timeout != 0 {
		t.Fatalf("Timeout = %v", c.Timeout)
	}
	_ = c.Transport.(*HTTP3Transport).Close()
}

type staticHTTP3Resolver struct{}

func (staticHTTP3Resolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
}

func (staticHTTP3Resolver) LookupPort(context.Context, string, string) (int, error) {
	return 443, nil
}

func TestHTTP3DialUsesNonEarlyQUICTransport(t *testing.T) {
	sentinel := errors.New("dial reached")
	called := false
	d := &http3Dialer{
		resolver:  staticHTTP3Resolver{},
		listenUDP: net.ListenUDP,
		dialQUIC: func(_ *quicgo.Transport, _ context.Context, _ net.Addr, _ *tls.Config, _ *quicgo.Config) (*quicgo.Conn, error) {
			called = true
			return nil, sentinel
		},
	}
	defer d.Close()
	_, err := d.Dial(context.Background(), "example.test:443", &tls.Config{}, &quicgo.Config{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Dial = %v", err)
	}
	if !called {
		t.Fatal("non-early quic.Transport.Dial seam was not reached")
	}
}

func TestMapHTTP3ErrorPreservesContext(t *testing.T) {
	if err := mapHTTP3Error(context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v", err)
	}
	if err := mapHTTP3Error(context.DeadlineExceeded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline = %v", err)
	}
}

func TestMapHTTP3ErrorClassifiesProtocolAndClosed(t *testing.T) {
	p := &http3.Error{}
	err := mapHTTP3Error(p)
	var h3err *HTTP3Error
	if !errors.As(err, &h3err) || h3err.Kind != HTTP3ErrorProtocol {
		t.Fatalf("protocol = %#v", err)
	}

	err = mapHTTP3Error(http3.ErrTransportClosed)
	if !errors.Is(err, ErrHTTP3TransportClosed) {
		t.Fatalf("closed = %v", err)
	}
	if !errors.As(err, &h3err) || h3err.Kind != HTTP3ErrorClosed {
		t.Fatalf("closed kind = %#v", err)
	}
}
