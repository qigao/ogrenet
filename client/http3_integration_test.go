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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

func startHTTP3Server(t testing.TB, handler http.Handler, configure func(*http3.Server)) (string, *tls.Config) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
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
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv}},
		MinVersion:   tls.VersionTLS13,
	}

	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := quicgo.Listen(pc, http3.ConfigureTLSConfig(serverTLS), &quicgo.Config{MaxIdleTimeout: 5 * time.Second})
	if err != nil {
		_ = pc.Close()
		t.Fatal(err)
	}
	server := &http3.Server{Handler: handler}
	if configure != nil {
		configure(server)
	}
	go func() {
		_ = server.ServeListener(ln)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		_ = ln.Close()
		_ = pc.Close()
	})

	return "https://" + ln.Addr().String(), &tls.Config{
		RootCAs:    roots,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS13,
	}
}

func newHTTP3TestClient(t testing.TB, tlsCfg *tls.Config) (*http.Client, *HTTP3Transport) {
	t.Helper()
	tr, err := NewHTTP3Transport(HTTP3Config{TLSConfig: tlsCfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return &http.Client{Transport: tr}, tr
}

func TestHTTP3Loopback(t *testing.T) {
	url, tlsCfg := startHTTP3Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proto", r.Proto)
		_, _ = io.WriteString(w, "ok")
	}), nil)
	client, _ := newHTTP3TestClient(t, tlsCfg)

	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 3 {
		t.Fatalf("protocol = %s, want HTTP/3", resp.Proto)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ok" {
		t.Fatalf("body = %q", body)
	}
}

func TestHTTP3ResponseStreaming(t *testing.T) {
	release := make(chan struct{})
	url, tlsCfg := startHTTP3Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("HTTP/3 response writer does not implement http.Flusher")
			return
		}
		_, _ = io.WriteString(w, "first")
		flusher.Flush()
		<-release
		_, _ = io.WriteString(w, "second")
	}), nil)
	client, _ := newHTTP3TestClient(t, tlsCfg)

	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, len("first"))
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	if string(first) != "first" {
		t.Fatalf("first chunk = %q", first)
	}
	close(release)
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != "second" {
		t.Fatalf("remaining body = %q", rest)
	}
}

func TestHTTP3RequestStreaming(t *testing.T) {
	firstSeen := make(chan struct{})
	bodySeen := make(chan string, 1)
	url, tlsCfg := startHTTP3Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first := make([]byte, len("first"))
		if _, err := io.ReadFull(r.Body, first); err != nil {
			bodySeen <- "read error: " + err.Error()
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		close(firstSeen)
		rest, err := io.ReadAll(r.Body)
		if err != nil {
			bodySeen <- "read error: " + err.Error()
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		bodySeen <- string(first) + string(rest)
		w.WriteHeader(http.StatusNoContent)
	}), nil)
	client, _ := newHTTP3TestClient(t, tlsCfg)

	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, url, pr)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		result <- err
	}()
	if _, err := io.WriteString(pw, "first"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe first request-body chunk")
	}
	if _, err := io.WriteString(pw, "second"); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got := <-bodySeen; got != "firstsecond" {
		t.Fatalf("request body = %q", got)
	}
}

func TestHTTP3RequestContextCancellation(t *testing.T) {
	entered := make(chan struct{})
	url, tlsCfg := startHTTP3Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	}), nil)
	client, _ := newHTTP3TestClient(t, tlsCfg)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := client.Do(req)
		result <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("request error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request did not cancel promptly")
	}
}

func TestHTTP3ConcurrentMultiplexAndReuse(t *testing.T) {
	var connections atomic.Int32
	url, tlsCfg := startHTTP3Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}), func(s *http3.Server) {
		s.ConnContext = func(ctx context.Context, conn *quicgo.Conn) context.Context {
			connections.Add(1)
			return ctx
		}
	})
	client, _ := newHTTP3TestClient(t, tlsCfg)

	const concurrent = 16
	var wg sync.WaitGroup
	errs := make(chan error, concurrent)
	wg.Add(concurrent)
	for range concurrent {
		go func() {
			defer wg.Done()
			resp, err := client.Get(url)
			if err != nil {
				errs <- err
				return
			}
			_, err = io.Copy(io.Discard, resp.Body)
			closeErr := resp.Body.Close()
			if err != nil {
				errs <- err
				return
			}
			if closeErr != nil {
				errs <- closeErr
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("connections after concurrent requests = %d, want 1", got)
	}

	for range 2 {
		resp, err := client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("connections after sequential reuse = %d, want 1", got)
	}
}
