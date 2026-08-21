package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestObservabilityRaceStatsVsSendClose(t *testing.T) {
	e, err := New(WithWriteQueue(32))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	defer right.Close()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	peerDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, right)
		close(peerDone)
	}()

	stopStats, statsDone := startSessionStatsReader(c)
	payload := ogrenet.Bin([]byte("race-send-close"))
	for i := 0; i < 128; i++ {
		err := c.TrySend(payload)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrWouldBlock) {
			runtime.Gosched()
			continue
		}
		t.Fatalf("TrySend: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, c.Done(), "session Done")
	close(stopStats)
	<-statsDone
	if got := c.Stats(); got.QueuedFrames != 0 || got.QueuedBytes != 0 {
		t.Fatalf("final queue gauges=%+v", got)
	}
	_ = right.Close()
	waitClosed(t, peerDone, "peer drain")
}

func TestObservabilityRaceObserverSaturationVsShutdown(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	e, err := New(
		WithObserverBuffer(1),
		WithObserver(ogrenet.ObserverFunc(func(ogrenet.Event) {
			once.Do(func() { close(entered) })
			<-release
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	if !e.observer.emit(ogrenet.Event{Kind: ogrenet.EventRead}) {
		t.Fatal("first event was not accepted")
	}
	waitClosed(t, entered, "observer entry")
	if !e.observer.emit(ogrenet.Event{Kind: ogrenet.EventWrite}) {
		t.Fatal("buffered event was not accepted")
	}
	if e.observer.emit(ogrenet.Event{Kind: ogrenet.EventClose}) {
		t.Fatal("overflow event unexpectedly accepted")
	}
	if e.Stats().ObserverDroppedEvents == 0 {
		t.Fatal("observer drop was not counted")
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, e.Done(), "engine Done with blocked observer")
	close(release)
}

func TestObservabilityRaceListenerAcceptRejectVsClose(t *testing.T) {
	server, err := New(WithLimits(Limits{MaxConnectionsPerListener: 1}))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	accepted := make(chan ogrenet.Session, 1)
	listener, err := server.Listen(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.HandlerFuncs{Open: func(s ogrenet.Session) { accepted <- s }})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Dial(context.Background(), listener.Endpoint(), ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	var serverSession ogrenet.Session
	select {
	case serverSession = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("first accept timeout")
	}
	defer serverSession.Close()

	stopStats := make(chan struct{})
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		for {
			select {
			case <-stopStats:
				return
			default:
				_ = listener.Stats()
				runtime.Gosched()
			}
		}
	}()

	raw, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		close(stopStats)
		<-statsDone
		t.Fatal(err)
	}
	defer raw.Close()
	deadline := time.Now().Add(2 * time.Second)
	for listener.Stats().RejectedConnections == 0 {
		if time.Now().After(deadline) {
			close(stopStats)
			<-statsDone
			t.Fatal("listener rejection was not observed")
		}
		runtime.Gosched()
	}

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, listener.Done(), "listener Done")
	close(stopStats)
	<-statsDone
	if got := listener.Stats(); got.AcceptedConnections != 1 || got.RejectedConnections == 0 {
		t.Fatalf("listener final stats=%+v", got)
	}
}

func TestObservabilityRaceTerminalFailureVsStats(t *testing.T) {
	e, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	defer right.Close()
	wrapped := &observabilityFailWriteConn{
		Conn:    left,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	c, err := e.adoptStream(wrapped, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- c.Send(context.Background(), ogrenet.Bin([]byte("terminal-failure")))
	}()
	waitClosed(t, wrapped.entered, "blocked write")

	stopStats, statsDone := startSessionStatsReader(c)
	close(wrapped.release)
	if err := <-sendDone; err == nil {
		t.Fatal("Send unexpectedly succeeded")
	}
	waitClosed(t, c.Done(), "failed session Done")
	close(stopStats)
	<-statsDone
	if c.Err() == nil {
		t.Fatal("terminal failure was not retained")
	}
	final := c.Stats()
	if final.QueuedFrames != 0 || final.QueuedBytes != 0 || final.Age <= 0 {
		t.Fatalf("final stats=%+v", final)
	}
}

func TestObservabilityRaceStopVsEmit(t *testing.T) {
	d := newObserverDispatcher(ogrenet.ObserverFunc(func(ogrenet.Event) {}), 16)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				d.emit(ogrenet.Event{Kind: ogrenet.EventRead})
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		d.stop()
	}()
	close(start)
	wg.Wait()
	if d.emit(ogrenet.Event{Kind: ogrenet.EventWrite}) {
		t.Fatal("stopped dispatcher accepted event")
	}
}

func startSessionStatsReader(session ogrenet.Session) (chan struct{}, chan struct{}) {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = session.Stats()
				runtime.Gosched()
			}
		}
	}()
	return stop, done
}

func waitClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s timeout", name)
	}
}

type observabilityFailWriteConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *observabilityFailWriteConn) Write([]byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return 0, errors.New("observability forced write failure")
}
