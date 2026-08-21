package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/qigao/ogrenet"
)

func TestWebSocketPeerCloseFailsQueuedSendAndConverges(t *testing.T) {
	endpoint, waitAccepted, triggerClose := startPeerClosingWebSocketServer(t)
	cfg := testWebSocketCloseConfig(time.Second)
	cfg.WriteTimeout = 2 * time.Second
	client, err := New(WithWebSocketConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	session, err := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitAccepted()
	ws := session.(*wsSession)

	activeResult := make(chan error, 1)
	go func() {
		activeResult <- session.Send(context.Background(), ogrenet.Bin(make([]byte, 4<<20)))
	}()
	waitWSWriteActive(t, ws)

	queuedResult := make(chan error, 1)
	go func() {
		queuedResult <- session.Send(context.Background(), ogrenet.Text("queued-after-active-write"))
	}()
	waitWSQueueDepth(t, ws, 1)

	triggerClose()

	select {
	case err := <-queuedResult:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("queued Send = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued Send did not converge after peer close")
	}

	select {
	case err := <-activeResult:
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Fatalf("active Send = %v, want nil or ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active Send did not converge after peer close")
	}

	waitClosed(t, session.Done(), "peer-closed WebSocket session")
	if err := session.Err(); err != nil {
		t.Fatalf("Session.Err = %v, want nil for normal peer close", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := session.Send(ctx, ogrenet.Text("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send after peer close = %v, want ErrClosed", err)
	}
}

func TestWebSocketSessionFamiliesExposeExpectedCapabilities(t *testing.T) {
	for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeWS, ogrenet.SchemeWSS} {
		p := dialSessionPair(t, scheme)
		if _, ok := p.client.(ogrenet.HalfCloseSession); ok {
			p.close()
			t.Fatalf("%s session unexpectedly implements HalfCloseSession", scheme)
		}
		if _, ok := p.client.(ogrenet.Session); !ok {
			p.close()
			t.Fatalf("%s session does not implement Session", scheme)
		}
		p.close()
	}
}

func TestWSSShutdownCallerDeadlineUsesPhysicalTransport(t *testing.T) {
	endpoint, clientTLS, waitAccepted := startNonResponsiveWSServer(t)
	client, err := New(
		WithTLSClientConfig(clientTLS),
		WithWebSocketConfig(testWebSocketCloseConfig(2*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	session, err := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitAccepted()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- session.Shutdown(ctx) }()

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WSS Shutdown = %v, want caller deadline", err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("WSS Shutdown did not physically abort on caller deadline")
	}
	waitClosed(t, session.Done(), "caller-aborted WSS session")
	if err := session.Err(); err != nil {
		t.Fatalf("WSS Session.Err = %v, want nil after caller abort", err)
	}
}

func TestWSSExplicitCloseInterruptsGracefulHandshake(t *testing.T) {
	endpoint, clientTLS, waitAccepted := startNonResponsiveWSServer(t)
	client, err := New(
		WithTLSClientConfig(clientTLS),
		WithWebSocketConfig(testWebSocketCloseConfig(2*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	session, err := client.Dial(context.Background(), endpoint, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitAccepted()
	internal := session.(*wsSession)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- session.Shutdown(ctx) }()
	waitClosed(t, internal.writerDrained, "WSS writer drain")

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("WSS Shutdown racing Close = %v, want ErrClosed", err)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("WSS Close did not interrupt graceful close handshake")
	}
	waitClosed(t, session.Done(), "explicitly aborted WSS session")
	if err := session.Err(); err != nil {
		t.Fatalf("WSS Session.Err = %v, want nil after explicit Close", err)
	}
}

func TestWSSServerCloseTimeoutUsesPhysicalTransport(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	server, err := New(
		WithTLSServerConfig(serverTLS),
		WithWebSocketConfig(testWebSocketCloseConfig(50*time.Millisecond)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	accepted := make(chan ogrenet.Session, 1)
	ln, err := server.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeWSS, Host: "127.0.0.1", Port: 0, Path: "/"}, ogrenet.HandlerFuncs{
		Open: func(s ogrenet.Session) { accepted <- s },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS.Clone()}}
	peer, _, err := websocket.Dial(context.Background(), ln.Endpoint().URL(), &websocket.DialOptions{
		HTTPClient:      httpClient,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.CloseNow()

	var session ogrenet.Session
	select {
	case session = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("server WSS session did not open")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	err = session.Shutdown(ctx)
	var te *TimeoutError
	if !errors.As(err, &te) || te.Kind != TimeoutClose {
		t.Fatalf("server WSS Shutdown = %#v, want TimeoutClose", err)
	}
	waitClosed(t, session.Done(), "server timeout-closed WSS session")
}

func waitWSWriteActive(t *testing.T, s *wsSession) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.writeState.mu.RLock()
		active := s.writeState.ctx != nil
		s.writeState.mu.RUnlock()
		if active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("WebSocket writer did not enter active write")
}

func waitWSQueueDepth(t *testing.T, s *wsSession, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(s.queue) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("WebSocket queue depth = %d, want >= %d", len(s.queue), want)
}

func startPeerClosingWebSocketServer(t *testing.T) (ogrenet.Endpoint, func(), func()) {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := smallReadBufferListener{Listener: raw}
	accepted := make(chan *websocket.Conn, 1)
	closePeer := make(chan struct{})
	serverDone := make(chan struct{})

	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		accepted <- ws
		<-closePeer
		_ = ws.Close(websocket.StatusNormalClosure, "")
	})}
	go func() {
		_ = server.Serve(ln)
		close(serverDone)
	}()

	var peer *websocket.Conn
	var acceptOnce, closeOnce sync.Once
	waitAccepted := func() {
		t.Helper()
		acceptOnce.Do(func() {
			select {
			case peer = <-accepted:
			case <-time.After(2 * time.Second):
				t.Fatal("peer-close WebSocket server did not accept")
			}
		})
	}
	triggerClose := func() { closeOnce.Do(func() { close(closePeer) }) }

	t.Cleanup(func() {
		triggerClose()
		if peer != nil {
			_ = peer.CloseNow()
		}
		_ = server.Close()
		_ = raw.Close()
		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Error("peer-close WebSocket server did not stop")
		}
	})

	addr := raw.Addr().(*net.TCPAddr)
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeWS, Host: "127.0.0.1", Port: uint16(addr.Port), Path: "/"}, waitAccepted, triggerClose
}

func startNonResponsiveWSServer(t *testing.T) (ogrenet.Endpoint, *tls.Config, func()) {
	t.Helper()
	serverTLS, clientTLS := testTLSConfigs(t)
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := tls.NewListener(raw, serverTLS.Clone())
	accepted := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	serverDone := make(chan struct{})

	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		accepted <- ws
		<-release
		_ = ws.CloseNow()
	})}
	go func() {
		_ = server.Serve(ln)
		close(serverDone)
	}()

	var peer *websocket.Conn
	var acceptOnce, releaseOnce sync.Once
	waitAccepted := func() {
		t.Helper()
		acceptOnce.Do(func() {
			select {
			case peer = <-accepted:
			case <-time.After(2 * time.Second):
				t.Fatal("non-responsive WSS server did not accept")
			}
		})
	}

	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if peer != nil {
			_ = peer.CloseNow()
		}
		_ = server.Close()
		_ = raw.Close()
		select {
		case <-serverDone:
		case <-time.After(2 * time.Second):
			t.Error("non-responsive WSS server did not stop")
		}
	})

	addr := raw.Addr().(*net.TCPAddr)
	return ogrenet.Endpoint{Scheme: ogrenet.SchemeWSS, Host: "127.0.0.1", Port: uint16(addr.Port), Path: "/"}, clientTLS, waitAccepted
}
