//go:build linux

package transport

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/epoll"
	"golang.org/x/sys/unix"
)

func TestEpollNativePacketPhysicalWriteIsReactorOwned(t *testing.T) {
	_, _, client := newEpollPacketPair(t)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	client.reactor.signal(newTestInboxItem(func(*epollReactor) {
		close(entered)
		<-release
	}))
	waitNativeSendSignal(t, entered, "packet reactor blocker")

	called := make(chan struct{}, 1)
	client.testWriteDatagram = func(req packetOutbound) (int, error) {
		called <- struct{}{}
		return len(req.data), nil
	}
	packet := ogrenet.Packet{Data: []byte("reactor-owned")}
	if err := client.TrySend(packet); err != nil {
		t.Fatalf("TrySend: %v", err)
	}

	select {
	case <-called:
		t.Fatal("physical datagram write ran while owning reactor was blocked")
	default:
	}

	releaseOnce.Do(func() { close(release) })
	waitNativeSendSignal(t, called, "reactor-owned physical datagram write")
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := client.Stats()
		if stats.PacketsTX == 1 && stats.QueuedPackets == 0 && stats.QueuedBytes == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("reactor-owned write did not release admission: %+v", client.Stats())
}

func TestEpollNativePacketEINTRRetriesCurrentDatagramWithoutReadmission(t *testing.T) {
	_, _, client := newEpollPacketPair(t)
	var transfers atomic.Int32
	client.testAfterPacketQueueTransfer = func(packetOutbound) {
		transfers.Add(1)
	}

	generations := make(chan uint64, 2)
	var calls atomic.Int32
	client.testWriteDatagram = func(req packetOutbound) (int, error) {
		generations <- client.writeGen
		if calls.Add(1) == 1 {
			return 0, unix.EINTR
		}
		return len(req.data), nil
	}

	packet := ogrenet.Packet{Data: []byte("eintr-owned")}
	if err := client.Send(context.Background(), packet); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("physical write calls=%d, want 2", got)
	}
	if got := transfers.Load(); got != 1 {
		t.Fatalf("queue transfers=%d, want 1", got)
	}
	first, second := <-generations, <-generations
	if first == 0 || second != first {
		t.Fatalf("write generation changed across EINTR retry: first=%d second=%d", first, second)
	}
	stats := client.Stats()
	if stats.PacketsTX != 1 || stats.BytesTX != uint64(len(packet.Data)) || stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("EINTR retry stats=%+v", stats)
	}
}

func TestEpollNativePacketEAGAINYieldsThenReadinessRetriesWithoutReadmission(t *testing.T) {
	_, _, client := newEpollPacketPair(t)
	var transfers atomic.Int32
	client.testAfterPacketQueueTransfer = func(packetOutbound) {
		transfers.Add(1)
	}

	firstAttempt := make(chan struct{})
	yielded := make(chan struct{})
	generations := make(chan uint64, 2)
	var calls atomic.Int32
	client.testWriteDatagram = func(req packetOutbound) (int, error) {
		generations <- client.writeGen
		if calls.Add(1) == 1 {
			close(firstAttempt)
			client.reactor.signal(newTestInboxItem(func(r *epollReactor) {
				close(yielded)
				client.onReactorEvent(r, epoll.Writable)
			}))
			return 0, unix.EAGAIN
		}
		return len(req.data), nil
	}

	result := make(chan error, 1)
	packet := ogrenet.Packet{Data: []byte("eagain-owned")}
	go func() {
		result <- client.Send(context.Background(), packet)
	}()
	waitNativeSendSignal(t, firstAttempt, "first EAGAIN datagram attempt")
	waitNativeSendSignal(t, yielded, "same-reactor work after EAGAIN yield")
	if err := waitNativeSendResult(t, result, "datagram readiness retry"); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("physical write calls=%d, want 2", got)
	}
	if got := transfers.Load(); got != 1 {
		t.Fatalf("queue transfers=%d, want 1", got)
	}
	first, second := <-generations, <-generations
	if first == 0 || second != first {
		t.Fatalf("write generation changed across EAGAIN readiness retry: first=%d second=%d", first, second)
	}
	stats := client.Stats()
	if stats.PacketsTX != 1 || stats.BytesTX != uint64(len(packet.Data)) || stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("EAGAIN retry stats=%+v", stats)
	}
}

func TestEpollNativePacketRuntimeGenerationDoesNotRefreshCurrentWriteDeadline(t *testing.T) {
	_, client := newEpollBlockedPacketWriter(t, 120*time.Millisecond)
	firstAttempt := make(chan uint64, 1)
	client.testWriteDatagram = func(packetOutbound) (int, error) {
		select {
		case firstAttempt <- client.writeGen:
		default:
		}
		return 0, unix.EAGAIN
	}

	result := make(chan error, 1)
	go func() {
		result <- client.Send(context.Background(), ogrenet.Packet{Data: []byte("fixed-write-deadline")})
	}()
	firstWriteGen := <-firstAttempt
	if firstWriteGen == 0 {
		t.Fatal("current datagram did not acquire write generation")
	}

	type generations struct {
		beforeRuntime uint64
		afterRuntime  uint64
		write         uint64
	}
	snapshot := make(chan generations, 1)
	client.reactor.signal(newTestInboxItem(func(*epollReactor) {
		before := client.nativePacketConnectionIdleGeneration()
		client.stats.bytesRX.Add(1)
		after := client.nativePacketConnectionIdleGeneration()
		snapshot <- generations{beforeRuntime: before, afterRuntime: after, write: client.writeGen}
	}))
	got := <-snapshot
	if got.afterRuntime == got.beforeRuntime {
		t.Fatalf("runtime deadline generation did not advance: before=%d after=%d", got.beforeRuntime, got.afterRuntime)
	}
	if got.write != firstWriteGen {
		t.Fatalf("runtime generation changed current write generation: first=%d after-runtime=%d", firstWriteGen, got.write)
	}

	err := waitNativeSendResult(t, result, "fixed datagram write deadline after runtime generation change")
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("Send error=%v, want fixed write timeout", err)
	}
	var timeoutErr *TimeoutError
	if !errors.As(err, &timeoutErr) || timeoutErr.Kind != TimeoutWrite {
		t.Fatalf("Send timeout=%T %v, want TimeoutWrite", err, err)
	}
}

func TestEpollNativePacketPositiveShortWriteIsTerminalWithoutContinuation(t *testing.T) {
	_, _, client := newEpollPacketPair(t)
	var calls atomic.Int32
	client.testWriteDatagram = func(req packetOutbound) (int, error) {
		calls.Add(1)
		return len(req.data) - 1, nil
	}

	packet := ogrenet.Packet{Data: []byte("short-write")}
	err := client.Send(context.Background(), packet)
	if err == nil {
		t.Fatal("Send succeeded after positive short datagram write")
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Send error=%v, want io.ErrShortWrite ownership", err)
	}
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.Op != OpWrite || opErr.Protocol != ogrenet.SchemeUDP {
		t.Fatalf("short-write operational error=%#v", opErr)
	}
	waitEpollPacketDone(t, client.Done(), "positive short datagram write close")
	if got := calls.Load(); got != 1 {
		t.Fatalf("positive short write calls=%d, want exactly 1", got)
	}
	stats := client.Stats()
	if stats.PacketsTX != 0 || stats.BytesTX != 0 || stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("short-write terminal stats=%+v", stats)
	}
	if client.Err() == nil {
		t.Fatal("positive short write did not become terminal PacketConn error")
	}
}
