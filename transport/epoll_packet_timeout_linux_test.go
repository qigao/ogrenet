//go:build linux

package transport

import (
	"errors"
	"testing"
	"time"
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
