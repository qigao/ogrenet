//go:build linux && !race

package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

const epollUDPStressProgressTimeout = 5 * time.Second

func makeEpollUDPStressPayload(kind byte, worker, seq int) []byte {
	size := 32
	if seq&1 != 0 {
		size = 512
	}
	payload := make([]byte, size)
	payload[0] = byte(seq & 1)
	payload[1] = kind
	binary.LittleEndian.PutUint32(payload[2:6], uint32(worker))
	binary.LittleEndian.PutUint32(payload[6:10], uint32(seq))
	for i := 10; i < len(payload); i++ {
		payload[i] = byte((worker*17 + seq*31 + i) % 251)
	}
	return payload
}

func runEpollUDPRawEcho(conn *net.UDPConn, done chan<- error) {
	buf := make([]byte, 2048)
	for {
		n, peer, err := conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				done <- nil
				return
			}
			done <- err
			return
		}
		if _, err := conn.WriteToUDP(buf[:n], peer); err != nil {
			done <- err
			return
		}
	}
}

func runEpollUDPConnectedStressClient(parent context.Context, pc ogrenet.PacketConn, recv <-chan []byte, worker, count int, backpressure *atomic.Uint64) error {
	for seq := 0; seq < count; seq++ {
		ctx, cancel := context.WithTimeout(parent, epollUDPStressProgressTimeout)
		payload := makeEpollUDPStressPayload('c', worker, seq)
		packet := ogrenet.Packet{Data: payload}

		var err error
		if seq&1 == 0 {
			err = pc.TrySend(packet)
			if errors.Is(err, ErrWouldBlock) {
				backpressure.Add(1)
				err = pc.Send(ctx, packet)
			}
		} else {
			err = pc.Send(ctx, packet)
		}
		if err != nil {
			cancel()
			return fmt.Errorf("connected worker=%d seq=%d send: %w", worker, seq, err)
		}

		select {
		case echoed := <-recv:
			if !bytes.Equal(echoed, payload) {
				cancel()
				return fmt.Errorf("connected worker=%d seq=%d echo mismatch", worker, seq)
			}
		case <-ctx.Done():
			cancel()
			return fmt.Errorf("connected worker=%d seq=%d receive: %w", worker, seq, context.Cause(ctx))
		}
		cancel()
	}
	return nil
}

func runEpollUDPUnconnectedStressClient(parent context.Context, endpoint *net.UDPAddr, worker, count int) error {
	conn, err := net.DialUDP("udp4", nil, cloneUDPAddr(endpoint))
	if err != nil {
		return fmt.Errorf("unconnected worker=%d dial: %w", worker, err)
	}
	defer conn.Close()
	buf := make([]byte, 2048)

	for seq := 0; seq < count; seq++ {
		select {
		case <-parent.Done():
			return context.Cause(parent)
		default:
		}
		payload := makeEpollUDPStressPayload('u', worker, seq)
		if err := conn.SetDeadline(time.Now().Add(epollUDPStressProgressTimeout)); err != nil {
			return fmt.Errorf("unconnected worker=%d seq=%d deadline: %w", worker, seq, err)
		}
		if _, err := conn.Write(payload); err != nil {
			return fmt.Errorf("unconnected worker=%d seq=%d write: %w", worker, seq, err)
		}
		n, err := conn.Read(buf)
		if err != nil {
			return fmt.Errorf("unconnected worker=%d seq=%d read: %w", worker, seq, err)
		}
		if !bytes.Equal(buf[:n], payload) {
			return fmt.Errorf("unconnected worker=%d seq=%d echo mismatch", worker, seq)
		}
	}
	return nil
}

func TestEpollNativeUDPStress(t *testing.T) {
	const (
		connectedClients   = 16
		connectedPackets   = 64
		unconnectedClients = 8
		unconnectedPackets = 128
	)

	rawEngine, err := NewEpoll(EpollConfig{
		Pollers:         4,
		CallbackWorkers: 4,
		CallbackQueue:   8,
		EventBatch:      128,
		IOBudgetBytes:   64 << 10,
		IOBudgetOps:     32,
	},
		WithWriteQueue(2),
		WithMaxQueuedBytes(8<<10),
		WithMaxDatagramBytes(2048),
		WithLimits(Limits{MaxQueuedBytesTotal: 1 << 20}),
	)
	if err != nil {
		t.Fatal(err)
	}
	e := rawEngine.(*epollEngine)
	t.Cleanup(func() {
		_ = e.Close()
		waitEpollEngineDone(t, e.Done())
	})

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawEcho, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	echoDone := make(chan error, 1)
	go runEpollUDPRawEcho(rawEcho, echoDone)
	t.Cleanup(func() { _ = rawEcho.Close() })
	echoAddr := rawEcho.LocalAddr().(*net.UDPAddr)
	echoEndpoint := ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: uint16(echoAddr.Port)}

	serverErr := make(chan error, unconnectedClients*unconnectedPackets)
	var serverBackpressure atomic.Uint64
	listener, err := e.ListenPacket(parent, ogrenet.Endpoint{
		Scheme: ogrenet.SchemeUDP,
		Host:   "127.0.0.1",
		Port:   0,
	}, ogrenet.PacketHandlerFuncs{
		Packet: func(pc ogrenet.PacketConn, peer net.Addr, packet ogrenet.Packet) {
			ctx, cancel := context.WithTimeout(parent, epollUDPStressProgressTimeout)
			defer cancel()
			var err error
			if len(packet.Data) != 0 && packet.Data[0]&1 == 0 {
				err = pc.TrySendTo(peer, packet)
				if errors.Is(err, ErrWouldBlock) {
					serverBackpressure.Add(1)
					err = pc.SendTo(ctx, peer, packet)
				}
			} else {
				err = pc.SendTo(ctx, peer, packet)
			}
			if err != nil {
				serverErr <- err
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listenerAddr := listener.LocalAddr().(*net.UDPAddr)

	clients := make([]ogrenet.PacketConn, 0, connectedClients)
	receivers := make([]chan []byte, 0, connectedClients)
	for i := 0; i < connectedClients; i++ {
		recv := make(chan []byte, 1)
		pc, err := e.DialPacket(parent, echoEndpoint, ogrenet.PacketHandlerFuncs{
			Packet: func(_ ogrenet.PacketConn, _ net.Addr, packet ogrenet.Packet) {
				recv <- append([]byte(nil), packet.Data...)
			},
		})
		if err != nil {
			t.Fatalf("DialPacket client %d: %v", i, err)
		}
		clients = append(clients, pc)
		receivers = append(receivers, recv)
	}

	start := make(chan struct{})
	errCh := make(chan error, connectedClients+unconnectedClients)
	var connectedBackpressure atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(connectedClients + unconnectedClients)
	for i := 0; i < connectedClients; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			if err := runEpollUDPConnectedStressClient(parent, clients[i], receivers[i], i, connectedPackets, &connectedBackpressure); err != nil {
				errCh <- err
			}
		}()
	}
	for i := 0; i < unconnectedClients; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			if err := runEpollUDPUnconnectedStressClient(parent, listenerAddr, i, unconnectedPackets); err != nil {
				errCh <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-serverErr:
		t.Fatalf("unconnected listener reply error: %v", err)
	default:
	}

	for i, pc := range clients {
		stats := pc.Stats()
		if stats.PacketsTX != connectedPackets || stats.PacketsRX != connectedPackets || stats.DroppedDatagrams != 0 || stats.QueuedPackets != 0 || stats.QueuedBytes != 0 {
			t.Fatalf("connected client %d final traffic stats=%+v", i, stats)
		}
	}
	listenerStats := listener.Stats()
	wantUnconnected := uint64(unconnectedClients * unconnectedPackets)
	if listenerStats.PacketsRX != wantUnconnected || listenerStats.PacketsTX != wantUnconnected || listenerStats.DroppedDatagrams != 0 || listenerStats.QueuedPackets != 0 || listenerStats.QueuedBytes != 0 {
		t.Fatalf("unconnected listener final traffic stats=%+v, want packets=%d", listenerStats, wantUnconnected)
	}

	for i, pc := range clients {
		if i&1 != 0 {
			continue
		}
		if err := pc.Close(); err != nil {
			t.Fatalf("explicit client Close %d: %v", i, err)
		}
		select {
		case <-pc.Done():
		case <-time.After(epollUDPStressProgressTimeout):
			t.Fatalf("waiting explicit client %d Done", i)
		}
		if err := pc.Err(); err != nil {
			t.Fatalf("explicit client %d Err=%v", i, err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(parent, epollUDPStressProgressTimeout)
	defer shutdownCancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Engine.Shutdown: %v", err)
	}
	select {
	case <-e.Done():
	case <-shutdownCtx.Done():
		t.Fatalf("waiting Engine.Done: %v", context.Cause(shutdownCtx))
	}
	for i, pc := range clients {
		select {
		case <-pc.Done():
		default:
			t.Fatalf("client %d not Done after Engine.Shutdown", i)
		}
		if err := pc.Err(); err != nil {
			t.Fatalf("client %d Err after shutdown=%v", i, err)
		}
	}
	select {
	case <-listener.Done():
	default:
		t.Fatal("ListenPacket not Done after Engine.Shutdown")
	}
	if err := listener.Err(); err != nil {
		t.Fatalf("ListenPacket Err after shutdown=%v", err)
	}
	assertEpollEngineZeroInvariants(t, e)

	if err := rawEcho.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close raw echo: %v", err)
	}
	select {
	case err := <-echoDone:
		if err != nil {
			t.Fatalf("raw echo loop: %v", err)
		}
	case <-time.After(epollUDPStressProgressTimeout):
		t.Fatal("raw echo loop did not stop")
	}

	t.Logf("UDP stress complete: %d connected + %d unconnected datagrams, connected backpressure=%d, listener backpressure=%d",
		connectedClients*connectedPackets,
		unconnectedClients*unconnectedPackets,
		connectedBackpressure.Load(),
		serverBackpressure.Load(),
	)
}
