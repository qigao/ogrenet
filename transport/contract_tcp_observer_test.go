package transport_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
	"github.com/qigao/ogrenet/wire"
)

func waitTCPObservedEvent(t *testing.T, ctx context.Context, events <-chan ogrenet.Event, what string, match func(ogrenet.Event) bool) ogrenet.Event {
	t.Helper()
	for {
		select {
		case event := <-events:
			if match(event) {
				return event
			}
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", what, context.Cause(ctx))
			return ogrenet.Event{}
		}
	}
}

type blockingEncodeFramer struct {
	codec   *wire.Codec
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingEncodeFramer) Encode(msg ogrenet.Message) ([]byte, error) {
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return f.codec.Encode(msg)
}

func (f *blockingEncodeFramer) DecodeOne(src []byte) (ogrenet.Message, int, error) {
	return f.codec.DecodeOne(src)
}

func runTCPObserverContracts(t *testing.T, f engineFactory) {
	t.Helper()

	t.Run("identity-read-write-close", func(t *testing.T) {
		ctx, cancel := contractContext(t)
		defer cancel()
		serverEvents := make(chan ogrenet.Event, 64)
		clientEvents := make(chan ogrenet.Event, 64)
		server := f.new(t, transport.WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { serverEvents <- event })))
		serverSide := newTCPContractCapture(nil)
		ln, err := server.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, serverSide)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()

		client := f.new(t, transport.WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { clientEvents <- event })))
		clientSide := newTCPContractCapture(nil)
		clientSession, err := client.Dial(ctx, ln.Endpoint(), clientSide)
		if err != nil {
			t.Fatal(err)
		}
		defer clientSession.Close()
		serverSession := recvContract(t, ctx, serverSide.opened, "observer server OnOpen")
		defer serverSession.Close()
		_ = recvContract(t, ctx, clientSide.opened, "observer client OnOpen")

		connect := waitTCPObservedEvent(t, ctx, clientEvents, "Connect event", func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventConnect })
		if connect.Resource != ogrenet.ResourceSession || connect.ResourceID != clientSession.ID() || connect.ParentID != 0 || connect.Protocol != ogrenet.SchemeTCP || connect.Err != nil || connect.Duration <= 0 {
			t.Fatalf("Connect event=%+v client=%d", connect, clientSession.ID())
		}
		accept := waitTCPObservedEvent(t, ctx, serverEvents, "Accept event", func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventAccept })
		if accept.Resource != ogrenet.ResourceSession || accept.ResourceID != serverSession.ID() || accept.ParentID != ln.Stats().ResourceID || accept.Protocol != ogrenet.SchemeTCP || accept.Err != nil {
			t.Fatalf("Accept event=%+v server=%d listener=%d", accept, serverSession.ID(), ln.Stats().ResourceID)
		}

		clientPayload := []byte("observer-client-write")
		serverPayload := []byte("observer-server-write-longer")
		if err := clientSession.Send(ctx, ogrenet.Bin(clientPayload)); err != nil {
			t.Fatal(err)
		}
		_ = recvContract(t, ctx, serverSide.messages, "observer server read")
		clientWrite := waitTCPObservedEvent(t, ctx, clientEvents, "client Write event", func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventWrite })
		serverRead := waitTCPObservedEvent(t, ctx, serverEvents, "server Read event", func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventRead })
		if clientWrite.ResourceID != clientSession.ID() || clientWrite.Bytes != uint64(len(clientPayload)) || clientWrite.Err != nil {
			t.Fatalf("client Write event=%+v", clientWrite)
		}
		if serverRead.ResourceID != serverSession.ID() || serverRead.Bytes != uint64(len(clientPayload)) || serverRead.Err != nil {
			t.Fatalf("server Read event=%+v", serverRead)
		}

		if err := serverSession.Send(ctx, ogrenet.Bin(serverPayload)); err != nil {
			t.Fatal(err)
		}
		_ = recvContract(t, ctx, clientSide.messages, "observer client read")
		serverWrite := waitTCPObservedEvent(t, ctx, serverEvents, "server Write event", func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventWrite })
		clientRead := waitTCPObservedEvent(t, ctx, clientEvents, "client Read event", func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventRead })
		if serverWrite.ResourceID != serverSession.ID() || serverWrite.Bytes != uint64(len(serverPayload)) || serverWrite.Err != nil {
			t.Fatalf("server Write event=%+v", serverWrite)
		}
		if clientRead.ResourceID != clientSession.ID() || clientRead.Bytes != uint64(len(serverPayload)) || clientRead.Err != nil {
			t.Fatalf("client Read event=%+v", clientRead)
		}

		if err := clientSession.Close(); err != nil {
			t.Fatal(err)
		}
		closeEvent := waitTCPObservedEvent(t, ctx, clientEvents, "client Close event", func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventClose })
		if closeEvent.ResourceID != clientSession.ID() || closeEvent.Protocol != ogrenet.SchemeTCP || closeEvent.Err != nil {
			t.Fatalf("client Close event=%+v", closeEvent)
		}
		stable := clientSession.Stats()
		if stable.QueuedFrames != 0 || stable.QueuedBytes != 0 {
			t.Fatalf("Close observed before stable queue state: %+v", stable)
		}
		time.Sleep(10 * time.Millisecond)
		if got := clientSession.Stats().Age; got != stable.Age {
			t.Fatalf("Age changed after observed Close: %v -> %v", stable.Age, got)
		}
	})

	t.Run("failed-connect-id-zero", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().(*net.TCPAddr)
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := contractContext(t)
		defer cancel()
		events := make(chan ogrenet.Event, 16)
		client := f.new(t, transport.WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { events <- event })))
		_, dialErr := client.Dial(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: addr.IP.String(), Port: uint16(addr.Port)}, ogrenet.HandlerFuncs{})
		if dialErr == nil {
			t.Fatal("Dial unexpectedly succeeded")
		}
		event := waitTCPObservedEvent(t, ctx, events, "failed Connect event", func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventConnect })
		if event.Resource != ogrenet.ResourceSession || event.ResourceID != 0 || event.Protocol != ogrenet.SchemeTCP || event.Err == nil || event.Duration <= 0 {
			t.Fatalf("failed Connect event=%+v", event)
		}
		if !errors.Is(event.Err, dialErr) && event.Err != dialErr {
			t.Fatalf("failed Connect event error=%v, Dial error=%v", event.Err, dialErr)
		}
	})

	t.Run("one-backpressure-event-per-attempt", func(t *testing.T) {
		rawListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer rawListener.Close()
		accepted := make(chan net.Conn, 1)
		go func() {
			conn, acceptErr := rawListener.Accept()
			if acceptErr == nil {
				accepted <- conn
			}
		}()

		entered := make(chan struct{})
		release := make(chan struct{})
		framer := &blockingEncodeFramer{codec: wire.New(nil), entered: entered, release: release}
		events := make(chan ogrenet.Event, 32)
		client := f.new(t,
			transport.WithFramerFactory(func() wire.Framer { return framer }),
			transport.WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) { events <- event })),
		)
		ctx, cancel := contractContext(t)
		defer cancel()
		addr := rawListener.Addr().(*net.TCPAddr)
		session, err := client.Dial(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: addr.IP.String(), Port: uint16(addr.Port)}, ogrenet.HandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		rawPeer := recvContract(t, ctx, accepted, "backpressure raw peer")
		defer rawPeer.Close()

		first := make(chan error, 1)
		go func() { first <- session.TrySend(ogrenet.Text("held-encode")) }()
		waitContractDone(t, ctx, entered, "held Encode entry")
		const attempts = 3
		for i := 0; i < attempts; i++ {
			if err := session.TrySend(ogrenet.Text("blocked")); !errors.Is(err, transport.ErrWouldBlock) {
				t.Fatalf("TrySend backpressure attempt %d=%v, want ErrWouldBlock", i, err)
			}
		}
		for i := 0; i < attempts; i++ {
			event := waitTCPObservedEvent(t, ctx, events, "Backpressure event", func(event ogrenet.Event) bool { return event.Kind == ogrenet.EventBackpressure })
			if event.ResourceID != session.ID() || event.Protocol != ogrenet.SchemeTCP || event.Err == nil {
				t.Fatalf("Backpressure event %d=%+v", i, event)
			}
		}
		if got := session.Stats().Backpressure; got != attempts {
			t.Fatalf("Backpressure stats=%d, want %d", got, attempts)
		}
		close(release)
		if err := recvContract(t, ctx, first, "held TrySend completion"); err != nil {
			t.Fatalf("held TrySend=%v", err)
		}
	})

	t.Run("blocking-observer-drops-without-stalling-tcp", func(t *testing.T) {
		ctx, cancel := contractContext(t)
		defer cancel()
		server := f.new(t)
		serverSide := newTCPContractCapture(ogrenet.HandlerFuncs{Message: func(s ogrenet.Session, msg ogrenet.Message) {
			_ = s.Send(context.Background(), msg)
		}})
		ln, err := server.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, serverSide)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()

		observerEntered := make(chan struct{})
		releaseObserver := make(chan struct{})
		var once sync.Once
		clientSide := newTCPContractCapture(nil)
		client := f.new(t,
			transport.WithObserverBuffer(1),
			transport.WithObserver(ogrenet.ObserverFunc(func(ogrenet.Event) {
				once.Do(func() { close(observerEntered) })
				<-releaseObserver
			})),
		)
		session, err := client.Dial(ctx, ln.Endpoint(), clientSide)
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		serverSession := recvContract(t, ctx, serverSide.opened, "blocking observer server OnOpen")
		defer serverSession.Close()
		_ = recvContract(t, ctx, clientSide.opened, "blocking observer client OnOpen")
		waitContractDone(t, ctx, observerEntered, "blocking Observer entry")

		for i := 0; i < 4; i++ {
			want := ogrenet.Text("observer-progress")
			if err := session.Send(ctx, want); err != nil {
				t.Fatalf("Send while Observer blocked: %v", err)
			}
			got := recvContract(t, ctx, clientSide.messages, "echo while Observer blocked")
			if string(got.Data) != string(want.Data) {
				t.Fatalf("echo while Observer blocked=%q want=%q", got.Data, want.Data)
			}
		}
		if client.Stats().ObserverDroppedEvents == 0 {
			t.Fatal("blocking Observer produced no dropped events")
		}
		close(releaseObserver)
	})

	t.Run("panic-observer-does-not-harm-session", func(t *testing.T) {
		ctx, cancel := contractContext(t)
		defer cancel()
		server := f.new(t)
		serverSide := newTCPContractCapture(ogrenet.HandlerFuncs{Message: func(s ogrenet.Session, msg ogrenet.Message) {
			_ = s.Send(context.Background(), msg)
		}})
		ln, err := server.Listen(ctx, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, serverSide)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()

		clientSide := newTCPContractCapture(nil)
		client := f.new(t, transport.WithObserver(ogrenet.ObserverFunc(func(ogrenet.Event) { panic("observer-test") })))
		session, err := client.Dial(ctx, ln.Endpoint(), clientSide)
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		serverSession := recvContract(t, ctx, serverSide.opened, "panic observer server OnOpen")
		defer serverSession.Close()
		_ = recvContract(t, ctx, clientSide.opened, "panic observer client OnOpen")
		if err := session.Send(ctx, ogrenet.Text("panic-observer-progress")); err != nil {
			t.Fatal(err)
		}
		_ = recvContract(t, ctx, clientSide.messages, "echo after Observer panic")
		waitTCPContractCondition(t, ctx, "Observer panic counter", func() bool { return client.Stats().ObserverPanics > 0 })
		if err := session.Err(); err != nil {
			t.Fatalf("Session Err after Observer panic=%v", err)
		}
	})
}

func TestPortableTCPObserverContracts(t *testing.T) {
	runTCPObserverContracts(t, portableFactory())
}
