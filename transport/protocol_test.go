package transport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestSessionProtocolsTextAndBinary(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	tests := []struct {
		name       string
		scheme     ogrenet.Scheme
		serverOpts []Option
		clientOpts []Option
		path       string
	}{
		{name: "tcp", scheme: ogrenet.SchemeTCP},
		{name: "tls", scheme: ogrenet.SchemeTLS, serverOpts: []Option{WithTLSServerConfig(serverTLS)}, clientOpts: []Option{WithTLSClientConfig(clientTLS)}},
		{name: "ws", scheme: ogrenet.SchemeWS, path: "/echo"},
		{name: "wss", scheme: ogrenet.SchemeWSS, path: "/echo", serverOpts: []Option{WithTLSServerConfig(serverTLS)}, clientOpts: []Option{WithTLSClientConfig(clientTLS)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			server, err := New(tt.serverOpts...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = server.Close() }()

			endpoint := ogrenet.Endpoint{Scheme: tt.scheme, Host: "127.0.0.1", Port: 0, Path: tt.path}
			if tt.scheme == ogrenet.SchemeWS || tt.scheme == ogrenet.SchemeWSS {
				if endpoint.Path == "" {
					endpoint.Path = "/"
				}
			}
			listener, err := server.Listen(ctx, endpoint, ogrenet.HandlerFuncs{
				Message: func(s ogrenet.Session, msg ogrenet.Message) {
					if err := s.Send(context.Background(), msg); err != nil && !errors.Is(err, ErrClosed) {
						t.Errorf("server echo: %v", err)
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()

			client, err := New(tt.clientOpts...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()

			received := make(chan ogrenet.Message, 2)
			session, err := client.Dial(ctx, listener.Endpoint(), ogrenet.HandlerFuncs{
				Message: func(_ ogrenet.Session, msg ogrenet.Message) {
					received <- ogrenet.Message{Type: msg.Type, Data: append([]byte(nil), msg.Data...)}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if session.Protocol() != tt.scheme {
				t.Fatalf("protocol = %s, want %s", session.Protocol(), tt.scheme)
			}

			want := []ogrenet.Message{
				ogrenet.Text("hello 世界"),
				ogrenet.Bin([]byte{0x00, 0x01, 0xff, 0x00}),
			}
			for _, msg := range want {
				if err := session.Send(ctx, msg); err != nil {
					t.Fatal(err)
				}
			}
			for _, expected := range want {
				select {
				case got := <-received:
					if got.Type != expected.Type || !bytes.Equal(got.Data, expected.Data) {
						t.Fatalf("got %+v, want %+v", got, expected)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			_ = session.Close()
			select {
			case <-session.Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestUDPDatagramEcho(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	serverSocket, err := server.ListenPacket(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{
		Packet: func(c ogrenet.PacketConn, peer net.Addr, packet ogrenet.Packet) {
			if err := c.SendTo(context.Background(), peer, packet); err != nil && !errors.Is(err, ErrClosed) {
				t.Errorf("UDP echo: %v", err)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = serverSocket.Close() }()
	if serverSocket.RemoteAddr() != nil {
		t.Fatal("ListenPacket unexpectedly has a remote address")
	}

	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	received := make(chan []byte, 1)
	clientSocket, err := client.DialPacket(ctx, serverSocket.Endpoint(), ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) {
			received <- append([]byte(nil), packet.Data...)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = clientSocket.Close() }()
	if clientSocket.RemoteAddr() == nil {
		t.Fatal("DialPacket has no remote address")
	}

	want := []byte{0x01, 0x00, 0xff, 0x42}
	if err := clientSocket.Send(ctx, ogrenet.Packet{Data: want}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if !bytes.Equal(got, want) {
			t.Fatalf("got %x, want %x", got, want)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestProtocolMismatchAndNoTLSFallback(t *testing.T) {
	ctx := context.Background()
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	udp := ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 1}
	if _, err := e.Listen(ctx, udp, nil); !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("Listen(udp) = %v, want ErrProtocolMismatch", err)
	}
	tcp := ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 1}
	if _, err := e.ListenPacket(ctx, tcp, nil); !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("ListenPacket(tcp) = %v, want ErrProtocolMismatch", err)
	}

	tlsEndpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeTLS, Host: "127.0.0.1", Port: 0}
	if _, err := e.Listen(ctx, tlsEndpoint, nil); !errors.Is(err, ErrTLSConfigRequired) {
		t.Fatalf("TLS Listen without config = %v, want ErrTLSConfigRequired", err)
	}
	wssEndpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeWSS, Host: "127.0.0.1", Port: 0, Path: "/"}
	if _, err := e.Listen(ctx, wssEndpoint, nil); !errors.Is(err, ErrTLSConfigRequired) {
		t.Fatalf("WSS Listen without config = %v, want ErrTLSConfigRequired", err)
	}
}

func testTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "ogrenet test"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}, &tls.Config{
		RootCAs:    roots,
		MinVersion: tls.VersionTLS13,
	}
}
