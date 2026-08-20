package client

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

func TestHTTP3NoFallbackToTCPHTTP(t *testing.T) {
	tcpServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "tcp-ok")
	}))
	defer tcpServer.Close()

	ordinary, err := tcpServer.Client().Get(tcpServer.URL)
	if err != nil {
		t.Fatalf("ordinary HTTPS precondition failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, ordinary.Body)
	_ = ordinary.Body.Close()

	tlsCfg := tcpServer.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	tr, err := NewHTTP3Transport(HTTP3Config{
		TLSConfig:        tlsCfg,
		HandshakeTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tcpServer.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err == nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		t.Fatal("HTTP/3 client silently fell back to TCP HTTP")
	}
}

func makeHTTP3MismatchTLS(t *testing.T, alpn string) (*tls.Config, *tls.Config) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv}},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{alpn},
	}, &tls.Config{
		RootCAs:    roots,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS13,
	}
}

func TestHTTP3ALPNMismatchIsTransportError(t *testing.T) {
	serverTLS, clientTLS := makeHTTP3MismatchTLS(t, "not-h3")
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	ln, err := quicgo.Listen(pc, serverTLS, &quicgo.Config{MaxIdleTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	tr, err := NewHTTP3Transport(HTTP3Config{
		TLSConfig:        clientTLS,
		HandshakeTimeout: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+ln.Addr().String(), nil)
	_, err = (&http.Client{Transport: tr}).Do(req)
	if err == nil {
		t.Fatal("ALPN mismatch unexpectedly succeeded")
	}
	var h3err *HTTP3Error
	if !errors.As(err, &h3err) || h3err.Kind != HTTP3ErrorTransport {
		t.Fatalf("ALPN error = %#v, want HTTP3ErrorTransport", err)
	}
}

type blockingHTTP3Resolver struct {
	entered chan struct{}
	once    sync.Once
}

func (r *blockingHTTP3Resolver) LookupPort(context.Context, string, string) (int, error) {
	return 443, nil
}

func (r *blockingHTTP3Resolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	r.once.Do(func() { close(r.entered) })
	<-ctx.Done()
	return nil, context.Cause(ctx)
}

func TestHTTP3DNSCancellationPreservesContext(t *testing.T) {
	tr, err := NewHTTP3Transport(HTTP3Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	resolver := &blockingHTTP3Resolver{entered: make(chan struct{})}
	tr.dialer.resolver = resolver

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.invalid:443/", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := (&http.Client{Transport: tr}).Do(req)
		result <- err
	}()
	select {
	case <-resolver.entered:
	case <-time.After(time.Second):
		t.Fatal("resolver was not entered")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("DNS cancellation = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DNS cancellation did not unblock request")
	}

	tr.dialer.mu.Lock()
	initialized := tr.dialer.transport != nil
	tr.dialer.mu.Unlock()
	if initialized {
		t.Fatal("UDP/QUIC transport initialized before DNS completed")
	}
}

func TestHTTP3BlackholeCloseIsDeterministic(t *testing.T) {
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	go func() {
		buf := make([]byte, 2048)
		for {
			if _, _, err := pc.ReadFromUDP(buf); err != nil {
				return
			}
		}
	}()

	tr, err := NewHTTP3Transport(HTTP3Config{HandshakeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+pc.LocalAddr().String(), nil)
	_, _ = (&http.Client{Transport: tr}).Do(req)
	cancel()

	closed := make(chan error, 1)
	go func() { closed <- tr.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked after failed QUIC connection")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestHTTP3CloseIdleConnectionsPreservesActiveRequest(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseRequest := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseRequest()
	var requests atomic.Int32
	url, tlsCfg := startHTTP3Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(entered)
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	}), nil)
	client, tr := newHTTP3TestClient(t, tlsCfg)

	result := make(chan error, 1)
	go func() {
		resp, err := client.Get(url)
		if resp != nil {
			_ = resp.Body.Close()
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("active request did not reach server")
	}
	tr.CloseIdleConnections()
	releaseRequest()
	if err := <-result; err != nil {
		t.Fatalf("active request after CloseIdleConnections = %v", err)
	}

	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("request after CloseIdleConnections = %v", err)
	}
	_ = resp.Body.Close()
}
