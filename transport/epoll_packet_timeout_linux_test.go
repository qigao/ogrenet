//go:build linux

package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestEpollNativePacketReadIdleClosesConnectedPacket(t *testing.T) {
	_, _, client := newEpollPacketPair(t, WithTimeouts(Timeouts{ReadIdle: 40 * time.Millisecond}))

	select {
	case <-client.Done():
	case <-time.After(750 * time.Millisecond):
		t.Fatal("connected UDP PacketConn did not close on ReadIdle timeout")
	}

	err := client.Err()
	var timeoutErr *TimeoutError
	if !errors.As(err, &timeoutErr) || timeoutErr.Kind != TimeoutReadIdle {
		t.Fatalf("PacketConn Err=%T %v, want TimeoutReadIdle", err, err)
	}
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.Op != OpReceive {
		t.Fatalf("PacketConn operational Err=%#v, want OpReceive", opErr)
	}
}

func TestEpollNativePacketReadProgressRefreshesReadIdle(t *testing.T) {
	_, server, client := newEpollPacketPair(t, WithTimeouts(Timeouts{ReadIdle: 120 * time.Millisecond}))

	time.Sleep(70 * time.Millisecond)
	if err := server.SendTo(context.Background(), client.LocalAddr(), ogrenet.Packet{Data: []byte("refresh-read-idle")}); err != nil {
		t.Fatalf("server SendTo: %v", err)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for client.Stats().PacketsRX == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if client.Stats().PacketsRX == 0 {
		t.Fatal("connected UDP client did not observe read progress")
	}

	select {
	case <-client.Done():
		t.Fatalf("connected UDP closed at original ReadIdle deadline after real read progress: %v", client.Err())
	case <-time.After(70 * time.Millisecond):
	}

	select {
	case <-client.Done():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("connected UDP did not close at refreshed ReadIdle deadline")
	}
	var timeoutErr *TimeoutError
	if err := client.Err(); !errors.As(err, &timeoutErr) || timeoutErr.Kind != TimeoutReadIdle {
		t.Fatalf("PacketConn Err=%T %v, want refreshed TimeoutReadIdle", err, err)
	}
}
