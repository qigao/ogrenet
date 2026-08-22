//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestEpollNativePacketRacePeerSnapshotSurvivesCallerMutation(t *testing.T) {
	e := newEpollPacketTestEngine(t, 1, WithWriteQueue(2), WithMaxQueuedBytes(128))
	server, err := e.listenNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	server.reactor.signal(newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	}))
	waitEpollPacketDone(t, entered, "peer-snapshot reactor blocker")

	captured := make(chan *net.UDPAddr, 1)
	server.testWriteDatagram = func(req packetOutbound) (int, error) {
		captured <- cloneUDPAddr(req.peer)
		return len(req.data), nil
	}

	peer := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 30123}
	want := cloneUDPAddr(peer)
	payload := ogrenet.Packet{Data: []byte("peer-snapshot")}
	if err := server.TrySendTo(peer, payload); err != nil {
		t.Fatalf("TrySendTo: %v", err)
	}

	peer.Port = 40987
	peer.IP = net.ParseIP("127.0.0.2")
	releaseOnce.Do(func() { close(release) })

	var got *net.UDPAddr
	select {
	case got = <-captured:
	case <-time.After(2 * time.Second):
		t.Fatal("reactor did not consume peer snapshot")
	}
	if got == nil || got.String() != want.String() {
		t.Fatalf("reactor peer=%v, want publication snapshot %v", got, want)
	}

	barrier := make(chan struct{})
	server.reactor.signal(newTestInboxItem(func(*epollReactor) { close(barrier) }))
	waitEpollPacketDone(t, barrier, "peer-snapshot completion barrier")
	stats := server.Stats()
	if stats.PacketsTX != 1 || stats.BytesTX != uint64(len(payload.Data)) || stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("peer-snapshot final stats=%+v", stats)
	}
}

func TestEpollNativePacketRaceSignalDuringArmedWaitCannotBeLost(t *testing.T) {
	raw, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	e := newEpollPacketTestEngine(t, 1)
	peer := raw.LocalAddr().(*net.UDPAddr)
	client, err := e.dialNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   uint16(peer.Port),
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	installed := make(chan struct{})
	armed := make(chan struct{})
	releaseWait := make(chan struct{})
	wrote := make(chan struct{}, 1)
	var armOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseWait) }) })
	client.reactor.signal(newTestInboxItem(func(r *epollReactor) {
		client.testWriteDatagram = func(req packetOutbound) (int, error) {
			wrote <- struct{}{}
			return len(req.data), nil
		}
		r.testWaitArmed = func() {
			armOnce.Do(func() {
				close(armed)
				<-releaseWait
			})
		}
		close(installed)
	}))
	waitEpollPacketDone(t, installed, "wait-arm hook installation")
	waitEpollPacketDone(t, armed, "reactor armed wait")

	sendResult := make(chan error, 1)
	go func() {
		sendResult <- client.TrySend(ogrenet.Packet{Data: []byte("wake-before-wait")})
	}()
	select {
	case err := <-sendResult:
		if err != nil {
			t.Fatalf("TrySend while wait armed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TrySend blocked while reactor wait hook held")
	}

	releaseOnce.Do(func() { close(releaseWait) })
	waitEpollPacketDone(t, wrote, "packet write after pre-wait wake")
	barrier := make(chan struct{})
	client.reactor.signal(newTestInboxItem(func(r *epollReactor) {
		r.testWaitArmed = nil
		close(barrier)
	}))
	waitEpollPacketDone(t, barrier, "post-write reactor barrier")
	stats := client.Stats()
	if stats.PacketsTX != 1 || stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("wait-arm packet stats=%+v", stats)
	}
}

func TestEpollNativePacketRacePendingReadinessLosesToExplicitClose(t *testing.T) {
	e := newEpollPacketTestEngine(t, 1)
	packets := make(chan ogrenet.Packet, 4)
	closed := make(chan error, 1)
	server, err := e.listenNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{
		Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) {
			packets <- ogrenet.Packet{Data: append([]byte(nil), packet.Data...)}
		},
		Close: func(_ ogrenet.PacketConn, err error) { closed <- err },
	})
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	server.reactor.signal(newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	}))
	waitEpollPacketDone(t, entered, "read-close reactor blocker")

	target := server.LocalAddr().(*net.UDPAddr)
	raw, err := net.DialUDP("udp4", nil, cloneUDPAddr(target))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Write([]byte("pending-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Write([]byte("pending-b")); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })

	waitEpollPacketDone(t, server.Done(), "PacketConn Done after pending readiness close")
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("OnClose=%v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnClose not published")
	}
	if err := server.Err(); err != nil {
		t.Fatalf("PacketConn.Err=%v, want nil", err)
	}
	select {
	case packet := <-packets:
		t.Fatalf("pending readiness delivered packet after explicit Close won: %q", packet.Data)
	default:
	}
}

func TestEpollNativePacketRaceGlobalQuotaOwnershipSurvivesEngineDrain(t *testing.T) {
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	payload := []byte("quota-owned-before-drain")
	e := newEpollPacketTestEngine(t, 1,
		WithWriteQueue(2),
		WithMaxQueuedBytes(len(payload)*2),
		WithLimits(Limits{MaxQueuedBytesTotal: int64(len(payload))}),
	)
	peerAddr := peer.LocalAddr().(*net.UDPAddr)
	client, err := e.dialNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   uint16(peerAddr.Port),
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	client.reactor.signal(newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	}))
	waitEpollPacketDone(t, entered, "quota-drain reactor blocker")

	if err := client.TrySend(ogrenet.Packet{Data: payload}); err != nil {
		t.Fatalf("first TrySend: %v", err)
	}
	stats := client.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != uint64(len(payload)) || e.Stats().GlobalQueuedBytes != uint64(len(payload)) {
		t.Fatalf("admitted quota ownership packet=%+v engine=%+v", stats, e.Stats())
	}

	err = client.TrySend(ogrenet.Packet{Data: []byte("second")})
	if !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("second TrySend=%v, want ErrResourceExhausted", err)
	}
	var limitErr *LimitError
	if !errors.As(err, &limitErr) || limitErr.Kind != LimitQueuedBytes {
		t.Fatalf("second TrySend error=%#v, want LimitQueuedBytes", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- e.Shutdown(ctx) }()
	select {
	case <-client.gate.done():
	case <-ctx.Done():
		t.Fatalf("waiting for UDP drain gate: %v", context.Cause(ctx))
	}
	if err := client.TrySend(ogrenet.Packet{Data: []byte("after-drain")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("TrySend after drain gate=%v, want ErrClosed", err)
	}
	if got := e.Stats().GlobalQueuedBytes; got != uint64(len(payload)) {
		t.Fatalf("drain released admitted global ownership early: %d", got)
	}

	releaseOnce.Do(func() { close(release) })
	if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 128)
	n, _, err := peer.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read drained datagram: %v", err)
	}
	if string(buf[:n]) != string(payload) {
		t.Fatalf("drained datagram=%q, want %q", buf[:n], payload)
	}
	select {
	case err := <-shutdownResult:
		if err != nil {
			t.Fatalf("Engine.Shutdown=%v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for Engine.Shutdown: %v", context.Cause(ctx))
	}
	waitEpollEngineSignal(t, e.Done(), "Engine.Done after quota-owned UDP drain")
	if stats := client.Stats(); stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("final packet ownership=%+v", stats)
	}
	if got := e.Stats().GlobalQueuedBytes; got != 0 {
		t.Fatalf("final Engine GlobalQueuedBytes=%d, want 0", got)
	}
	assertEpollEngineZeroInvariants(t, e)
}
