package quic

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
)

const echoALPN = "ogrenet-quic-echo"

func TestStreamEcho(t *testing.T) {
	serverTLS, clientTLS := echoTLSConfigs(t)
	listener, err := quicgo.ListenAddr("127.0.0.1:0", serverTLS, &quicgo.Config{
		HandshakeIdleTimeout: 3 * time.Second,
		MaxIdleTimeout:       10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	clientClosed := make(chan struct{})
	go func() {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.CloseWithError(0, "")

		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		if _, err := io.Copy(stream, stream); err != nil {
			serverErr <- err
			return
		}
		if err := stream.Close(); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
		<-clientClosed
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, listener.Addr().String(), Config{
		TLSConfig:        clientTLS,
		ALPN:             echoALPN,
		HandshakeTimeout: 3 * time.Second,
		IdleTimeout:      10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	stream, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	message := []byte("hello over quic")
	if _, err := stream.Write(message); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	echoed, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	if string(echoed) != string(message) {
		t.Fatalf("echo = %q, want %q", echoed, message)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	close(clientClosed)
}

func echoTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	server := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{echoALPN},
	}
	client := &tls.Config{
		RootCAs:    roots,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS13,
	}
	return server, client
}
