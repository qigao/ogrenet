package transport_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
)

type tcpContractListenerHarness struct {
	ctx      context.Context
	cancel   context.CancelFunc
	engine   ogrenet.Engine
	listener ogrenet.Listener
	side     *tcpContractCapture
}

func newTCPContractListenerHarness(t *testing.T, f engineFactory, opts ...transport.Option) *tcpContractListenerHarness {
	t.Helper()
	ctx, cancel := contractContext(t)
	e := f.new(t, opts...)
	side := newTCPContractCapture(nil)
	ln, err := e.Listen(ctx, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeTCP,
		Host:   "127.0.0.1",
		Port:   0,
	}, side)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	h := &tcpContractListenerHarness{ctx: ctx, cancel: cancel, engine: e, listener: ln, side: side}
	t.Cleanup(func() {
		_ = h.listener.Close()
		h.cancel()
	})
	return h
}

func requireLimitKind(t *testing.T, err error, kind transport.LimitKind) *transport.LimitError {
	t.Helper()
	if !errors.Is(err, transport.ErrResourceExhausted) {
		t.Fatalf("error=%v, want ErrResourceExhausted", err)
	}
	var limitErr *transport.LimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("error=%T %v, want *LimitError", err, err)
	}
	if limitErr.Kind != kind {
		t.Fatalf("limit kind=%v, want %v", limitErr.Kind, kind)
	}
	return limitErr
}

func waitTCPContractCondition(t *testing.T, ctx context.Context, what string, condition func() bool) {
	t.Helper()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", what, context.Cause(ctx))
		case <-time.After(time.Millisecond):
		}
	}
}

func runTCPLimitStatsContracts(t *testing.T, f engineFactory) {
	t.Helper()

	t.Run("max-connections", func(t *testing.T) {
		server := newTCPContractListenerHarness(t, f)
		clientEngine := f.new(t, transport.WithLimits(transport.Limits{MaxConnections: 1}))

		first, err := clientEngine.Dial(server.ctx, server.listener.Endpoint(), ogrenet.HandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = first.Close() })
		serverFirst := recvContract(t, server.ctx, server.side.opened, "first max-connections server session")
		t.Cleanup(func() { _ = serverFirst.Close() })

		_, err = clientEngine.Dial(server.ctx, server.listener.Endpoint(), ogrenet.HandlerFuncs{})
		requireLimitKind(t, err, transport.LimitConnections)

		stats := clientEngine.Stats()
		if stats.ActiveConnections != 1 || stats.OpeningConnections != 0 || stats.RejectedConnections != 1 {
			t.Fatalf("client max-connections stats=%+v", stats)
		}

		if err := first.Close(); err != nil {
			t.Fatal(err)
		}
		waitContractDone(t, server.ctx, first.Done(), "first max-connections client Done")
		waitTCPContractCondition(t, server.ctx, "max-connections capacity release", func() bool {
			stats := clientEngine.Stats()
			return stats.ActiveConnections == 0 && stats.OpeningConnections == 0 && stats.DrainingConnections == 0
		})
	})

	t.Run("max-connections-per-peer", func(t *testing.T) {
		server := newTCPContractListenerHarness(t, f)
		clientEngine := f.new(t, transport.WithLimits(transport.Limits{MaxConnectionsPerPeer: 1}))

		first, err := clientEngine.Dial(server.ctx, server.listener.Endpoint(), ogrenet.HandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = first.Close() })
		serverFirst := recvContract(t, server.ctx, server.side.opened, "first per-peer server session")
		t.Cleanup(func() { _ = serverFirst.Close() })

		_, err = clientEngine.Dial(server.ctx, server.listener.Endpoint(), ogrenet.HandlerFuncs{})
		requireLimitKind(t, err, transport.LimitConnectionsPerPeer)
		stats := clientEngine.Stats()
		if stats.ActiveConnections != 1 || stats.RejectedPeers != 1 {
			t.Fatalf("client per-peer stats=%+v", stats)
		}
	})

	t.Run("max-connections-per-listener", func(t *testing.T) {
		server := newTCPContractListenerHarness(t, f, transport.WithLimits(transport.Limits{MaxConnectionsPerListener: 1}))
		clientEngine := f.new(t)
		client, err := clientEngine.Dial(server.ctx, server.listener.Endpoint(), ogrenet.HandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		serverFirst := recvContract(t, server.ctx, server.side.opened, "first per-listener server session")
		t.Cleanup(func() { _ = serverFirst.Close() })

		if got := server.listener.Stats().CurrentConnections; got != 1 {
			t.Fatalf("listener current=%d, want 1", got)
		}
		raw, err := net.Dial("tcp", server.listener.Addr().String())
		if err != nil {
			t.Fatalf("raw second connection: %v", err)
		}
		defer raw.Close()

		waitTCPContractCondition(t, server.ctx, "per-listener rejection", func() bool {
			return server.listener.Stats().RejectedConnections == 1
		})
		listenerStats := server.listener.Stats()
		engineStats := server.engine.Stats()
		if listenerStats.CurrentConnections != 1 || listenerStats.RejectedConnections != 1 {
			t.Fatalf("listener stats after rejection=%+v", listenerStats)
		}
		if engineStats.RejectedListeners != 1 || engineStats.ActiveConnections != 1 {
			t.Fatalf("server engine stats after listener rejection=%+v", engineStats)
		}
	})

	t.Run("global-queued-bytes", func(t *testing.T) {
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

		addr := rawListener.Addr().(*net.TCPAddr)
		clientEngine := f.new(t, transport.WithLimits(transport.Limits{MaxQueuedBytesTotal: 128}))
		ctx, cancel := contractContext(t)
		defer cancel()
		client, err := clientEngine.Dial(ctx, ogrenet.Endpoint{
			Scheme: ogrenet.SchemeTCP,
			Host:   addr.IP.String(),
			Port:   uint16(addr.Port),
		}, ogrenet.HandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		rawPeer := recvContract(t, ctx, accepted, "raw queued-bytes peer")
		defer rawPeer.Close()

		err = client.TrySend(ogrenet.Bin(bytes.Repeat([]byte{0x7a}, 1024)))
		requireLimitKind(t, err, transport.LimitQueuedBytes)
		stats := clientEngine.Stats()
		if stats.RejectedQueuedBytes != 1 || stats.GlobalQueuedBytes != 0 {
			t.Fatalf("queued-byte engine stats=%+v", stats)
		}
		sessionStats := client.Stats()
		if sessionStats.QueuedBytes != 0 || sessionStats.QueuedFrames != 0 {
			t.Fatalf("queued-byte session ownership leaked: %+v", sessionStats)
		}
	})

	t.Run("exact-stats-and-final-age", func(t *testing.T) {
		p := newTCPContractPair(t, f, nil, nil)
		clientPayload := []byte("client-to-server")
		serverPayload := []byte("server-to-client-longer")
		if err := p.client.Send(p.ctx, ogrenet.Bin(clientPayload)); err != nil {
			t.Fatal(err)
		}
		gotServer := recvContract(t, p.ctx, p.serverSide.messages, "server exact stats message")
		if !bytes.Equal(gotServer.Data, clientPayload) {
			t.Fatalf("server payload=%q, want %q", gotServer.Data, clientPayload)
		}
		if err := p.server.Send(p.ctx, ogrenet.Bin(serverPayload)); err != nil {
			t.Fatal(err)
		}
		gotClient := recvContract(t, p.ctx, p.clientSide.messages, "client exact stats message")
		if !bytes.Equal(gotClient.Data, serverPayload) {
			t.Fatalf("client payload=%q, want %q", gotClient.Data, serverPayload)
		}

		clientStats := p.client.Stats()
		serverStats := p.server.Stats()
		if clientStats.BytesTX != uint64(len(clientPayload)) || clientStats.MessagesTX != 1 || clientStats.BytesRX != uint64(len(serverPayload)) || clientStats.MessagesRX != 1 {
			t.Fatalf("client exact stats=%+v", clientStats)
		}
		if serverStats.BytesTX != uint64(len(serverPayload)) || serverStats.MessagesTX != 1 || serverStats.BytesRX != uint64(len(clientPayload)) || serverStats.MessagesRX != 1 {
			t.Fatalf("server exact stats=%+v", serverStats)
		}
		if clientStats.QueuedFrames != 0 || clientStats.QueuedBytes != 0 || serverStats.QueuedFrames != 0 || serverStats.QueuedBytes != 0 {
			t.Fatalf("non-zero queue gauges after blocking sends: client=%+v server=%+v", clientStats, serverStats)
		}
		if got := p.engine.Stats().GlobalQueuedBytes; got != 0 {
			t.Fatalf("engine global queued bytes=%d, want 0", got)
		}

		if err := p.client.Close(); err != nil {
			t.Fatal(err)
		}
		if err := p.server.Close(); err != nil {
			t.Fatal(err)
		}
		waitContractDone(t, p.ctx, p.client.Done(), "client exact stats Done")
		waitContractDone(t, p.ctx, p.server.Done(), "server exact stats Done")
		clientFinal := p.client.Stats()
		serverFinal := p.server.Stats()
		time.Sleep(20 * time.Millisecond)
		if got := p.client.Stats().Age; got != clientFinal.Age {
			t.Fatalf("client Age changed after Done: %v -> %v", clientFinal.Age, got)
		}
		if got := p.server.Stats().Age; got != serverFinal.Age {
			t.Fatalf("server Age changed after Done: %v -> %v", serverFinal.Age, got)
		}
		if clientFinal.QueuedFrames != 0 || clientFinal.QueuedBytes != 0 || serverFinal.QueuedFrames != 0 || serverFinal.QueuedBytes != 0 {
			t.Fatalf("final queue gauges non-zero: client=%+v server=%+v", clientFinal, serverFinal)
		}
	})
}

func TestPortableTCPLimitStatsContracts(t *testing.T) {
	runTCPLimitStatsContracts(t, portableFactory())
}
