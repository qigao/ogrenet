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
	"github.com/qigao/ogrenet/epoll"
	"golang.org/x/sys/unix"
)

func newEpollPacketPair(t *testing.T, opts ...Option) (*epollEngine, *epollPacketConn, *epollPacketConn) {
	t.Helper()
	e := newEpollPacketTestEngine(t, 1, opts...)
	server, err := e.listenNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	client, err := e.dialNativeUDP(context.Background(), ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   server.Endpoint().Port,
	}, ogrenet.PacketHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	return e, server, client
}

func newEpollBlockedPacketWriter(t *testing.T, writeTimeout time.Duration) (*epollEngine, *epollPacketConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	sendFD, peerFD := fds[0], fds[1]
	adopted := false
	defer func() {
		if !adopted {
			_ = unix.Close(sendFD)
			_ = unix.Close(peerFD)
		}
	}()
	t.Cleanup(func() { _ = unix.Close(peerFD) })

	if err := unix.SetsockoptInt(sendFD, unix.SOL_SOCKET, unix.SO_SNDBUF, 4096); err != nil {
		t.Fatal(err)
	}
	filler := make([]byte, 1024)
	full := false
	for i := 0; i < 1<<20; i++ {
		if _, err := unix.Write(sendFD, filler); err == nil {
			continue
		} else if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			full = true
			break
		} else {
			t.Fatalf("fill datagram send buffer: %v", err)
		}
	}
	if !full {
		t.Fatal("failed to drive nonblocking datagram socket to EAGAIN")
	}

	e := newEpollPacketTestEngine(t, 1, WithTimeouts(Timeouts{Write: writeTimeout}))
	id, err := e.nextResourceID()
	if err != nil {
		t.Fatal(err)
	}
	r := e.reactors[0]
	remote := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9}
	p := newEpollPacketConn(
		e,
		r,
		id,
		ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 9},
		nil,
		remote,
		ogrenet.PacketHandlerFuncs{},
		nil,
	)
	if err := e.addManaged(p); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	r.signal(newTestInboxItem(func(owner *epollReactor) {
		p.fd = sendFD
		p.local = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}
		p.state = epollPacketActive
		if err := owner.registerResource(p); err != nil {
			result <- err
			return
		}
		if err := owner.addFD(sendFD, epoll.Readable|epoll.Error|epoll.EdgeTriggered, p.id); err != nil {
			delete(owner.resources, p.id)
			result <- err
			return
		}
		p.registered = true
		result <- nil
	}))
	select {
	case err := <-result:
		if err != nil {
			e.removeManaged(p.id)
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		e.removeManaged(p.id)
		t.Fatal("timed out adopting blocked datagram fd")
	}
	adopted = true
	return e, p
}

func TestEpollNativePacketSendMethodLegalityAndOversize(t *testing.T) {
	_, server, client := newEpollPacketPair(t, WithMaxDatagramBytes(4))
	packet := ogrenet.Packet{Data: []byte("ping")}

	if err := server.Send(context.Background(), packet); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("unconnected Send error=%v, want ErrNotConnected", err)
	}
	if err := server.TrySend(packet); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("unconnected TrySend error=%v, want ErrNotConnected", err)
	}
	if err := server.SendTo(context.Background(), nil, packet); !errors.Is(err, ErrPeerRequired) {
		t.Fatalf("SendTo nil peer error=%v, want ErrPeerRequired", err)
	}
	if err := server.TrySendTo(nil, packet); !errors.Is(err, ErrPeerRequired) {
		t.Fatalf("TrySendTo nil peer error=%v, want ErrPeerRequired", err)
	}

	wrongPeer := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(server.Endpoint().Port) + 1}
	if err := client.TrySendTo(wrongPeer, packet); !errors.Is(err, ErrPeerMismatch) {
		t.Fatalf("connected TrySendTo wrong peer error=%v, want ErrPeerMismatch", err)
	}

	oversize := ogrenet.Packet{Data: []byte("12345")}
	err := client.TrySend(oversize)
	if !errors.Is(err, ErrDatagramTooLarge) {
		t.Fatalf("oversize TrySend error=%v, want ErrDatagramTooLarge", err)
	}
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.Op != OpSend || opErr.Protocol != ogrenet.SchemeUDP || opErr.Kind != ErrorTooLarge {
		t.Fatalf("oversize operational error=%#v", opErr)
	}
	stats := client.Stats()
	if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("oversize acquired queue ownership: %+v", stats)
	}

	if err := client.TrySendTo(client.RemoteAddr(), packet); err != nil {
		t.Fatalf("connected TrySendTo same peer: %v", err)
	}
}

func TestEpollNativePacketTrySendAdmissionAndBackpressure(t *testing.T) {
	backpressure := make(chan ogrenet.Event, 4)
	writes := make(chan ogrenet.Event, 4)
	e, _, client := newEpollPacketPair(t,
		WithWriteQueue(1),
		WithMaxQueuedBytes(64),
		WithLimits(Limits{MaxQueuedBytesTotal: 128}),
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
			if event.Resource != ogrenet.ResourcePacketConn {
				return
			}
			switch event.Kind {
			case ogrenet.EventBackpressure:
				select {
				case backpressure <- event:
				default:
				}
			case ogrenet.EventWrite:
				select {
				case writes <- event:
				default:
				}
			}
		})),
	)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	client.reactor.signal(newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	}))
	waitNativeSendSignal(t, entered, "packet reactor blocker")

	packet := ogrenet.Packet{Data: []byte("first")}
	if err := client.TrySend(packet); err != nil {
		t.Fatalf("first TrySend: %v", err)
	}
	stats := client.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != uint64(len(packet.Data)) {
		t.Fatalf("retained queue stats=%+v, want 1 packet/%d bytes", stats, len(packet.Data))
	}
	if got := e.Stats().GlobalQueuedBytes; got != uint64(len(packet.Data)) {
		t.Fatalf("Engine GlobalQueuedBytes=%d, want %d", got, len(packet.Data))
	}

	err := client.TrySend(ogrenet.Packet{Data: []byte("second")})
	if !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("second TrySend error=%v, want ErrWouldBlock", err)
	}
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.Op != OpSend || opErr.Kind != ErrorBackpressure {
		t.Fatalf("backpressure operational error=%#v", opErr)
	}
	if got := client.Stats().Backpressure; got != 1 {
		t.Fatalf("Backpressure=%d, want 1", got)
	}

	select {
	case event := <-backpressure:
		if event.ResourceID != client.id || event.Protocol != ogrenet.SchemeUDP || !errors.Is(event.Err, ErrWouldBlock) {
			t.Fatalf("EventBackpressure=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing EventBackpressure")
	}
	select {
	case extra := <-backpressure:
		t.Fatalf("duplicate EventBackpressure=%+v", extra)
	case <-time.After(25 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case event := <-writes:
		if event.ResourceID != client.id || event.Protocol != ogrenet.SchemeUDP || event.Bytes != uint64(len(packet.Data)) || event.Err != nil {
			t.Fatalf("EventWrite=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("first admitted datagram did not complete after reactor release")
	}
	stats = client.Stats()
	if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("queue ownership not released after write: %+v", stats)
	}
	if got := e.Stats().GlobalQueuedBytes; got != 0 {
		t.Fatalf("Engine GlobalQueuedBytes after write=%d, want 0", got)
	}
}

func TestEpollNativePacketSendCancellationAfterPublicationRetainsOwnership(t *testing.T) {
	writes := make(chan ogrenet.Event, 4)
	_, _, client := newEpollPacketPair(t,
		WithWriteQueue(2),
		WithMaxQueuedBytes(64),
		WithObserver(ogrenet.ObserverFunc(func(event ogrenet.Event) {
			if event.Kind == ogrenet.EventWrite && event.Resource == ogrenet.ResourcePacketConn {
				select {
				case writes <- event:
				default:
				}
			}
		})),
	)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	client.reactor.signal(newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	}))
	waitNativeSendSignal(t, entered, "packet reactor blocker")

	transferred := make(chan struct{})
	client.testAfterPacketQueueTransfer = func(packetOutbound) {
		close(transferred)
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	callerCause := errors.New("caller canceled after native packet queue transfer")
	result := make(chan error, 1)
	packet := ogrenet.Packet{Data: []byte("owned")}
	go func() {
		result <- client.Send(ctx, packet)
	}()

	waitNativeSendSignal(t, transferred, "native packet queue transfer")
	stats := client.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != uint64(len(packet.Data)) {
		t.Fatalf("Send did not retain admitted datagram after transfer: %+v", stats)
	}

	cancel(callerCause)
	if err := waitNativeSendResult(t, result, "canceled packet Send"); err != callerCause {
		t.Fatalf("Send error=%v, want exact caller cause %v", err, callerCause)
	}
	stats = client.Stats()
	if stats.QueuedPackets != 1 || stats.QueuedBytes != uint64(len(packet.Data)) {
		t.Fatalf("caller cancellation revoked admitted datagram: %+v", stats)
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case event := <-writes:
		if event.ResourceID != client.id || event.Protocol != ogrenet.SchemeUDP || event.Bytes != uint64(len(packet.Data)) || event.Err != nil {
			t.Fatalf("EventWrite=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted datagram was not physically written after caller cancellation")
	}
	stats = client.Stats()
	if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("admitted datagram ownership not released after physical write: %+v", stats)
	}
	if stats.PacketsTX != 1 || stats.BytesTX != uint64(len(packet.Data)) {
		t.Fatalf("TX stats after canceled Send=%+v", stats)
	}
}

func TestEpollNativePacketAdmissionCopiesPayloadBeforePublication(t *testing.T) {
	_, _, client := newEpollPacketPair(t, WithWriteQueue(2), WithMaxQueuedBytes(64))
	retained := make(chan []byte, 1)
	client.testAfterPacketQueueTransfer = func(req packetOutbound) {
		retained <- req.data
	}
	data := []byte("copy-owned")
	want := append([]byte(nil), data...)
	if err := client.TrySend(ogrenet.Packet{Data: data}); err != nil {
		t.Fatalf("TrySend: %v", err)
	}
	owned := <-retained

	for i := range data {
		data[i] = 'x'
	}
	if string(owned) != string(want) {
		t.Fatalf("retained datagram mutated with caller buffer: got=%q want=%q", owned, want)
	}
}

func TestEpollNativePacketWriteDeadlineFailsBlockedDatagramAndReleasesAdmission(t *testing.T) {
	e, client := newEpollBlockedPacketWriter(t, 75*time.Millisecond)
	result := make(chan error, 1)
	packet := ogrenet.Packet{Data: []byte("deadline-owned")}
	go func() {
		result <- client.Send(context.Background(), packet)
	}()

	select {
	case err := <-result:
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("Send error=%v, want ErrTimeout", err)
		}
		var timeoutErr *TimeoutError
		if !errors.As(err, &timeoutErr) || timeoutErr.Kind != TimeoutWrite {
			t.Fatalf("Send timeout=%T %v, want TimeoutWrite", err, err)
		}
		var opErr *Error
		if !errors.As(err, &opErr) || opErr.Op != OpWrite || opErr.Protocol != ogrenet.SchemeUDP || opErr.Kind != ErrorTimeout {
			t.Fatalf("Send operational error=%#v", opErr)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked native UDP datagram did not hit fixed write deadline")
	}

	waitEpollPacketDone(t, client.Done(), "blocked packet write timeout close")
	stats := client.Stats()
	if stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("write timeout retained packet ownership: %+v", stats)
	}
	if got := e.Stats().GlobalQueuedBytes; got != 0 {
		t.Fatalf("write timeout retained Engine queued bytes=%d", got)
	}
	if !errors.Is(client.Err(), ErrTimeout) {
		t.Fatalf("PacketConn.Err=%v, want write timeout", client.Err())
	}
}
