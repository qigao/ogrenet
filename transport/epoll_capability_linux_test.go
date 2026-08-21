//go:build linux

package transport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/qigao/ogrenet"
)

func TestEpollRejectsTLSWSWSSWithoutFallback(t *testing.T) {
	var observed atomic.Uint64
	e, err := NewEpoll(EpollConfig{Pollers: 1}, WithObserver(ogrenet.ObserverFunc(func(ogrenet.Event) {
		observed.Add(1)
	})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })

	for _, scheme := range []ogrenet.Scheme{ogrenet.SchemeTLS, ogrenet.SchemeWS, ogrenet.SchemeWSS} {
		ep := ogrenet.Endpoint{Scheme: scheme, Host: "127.0.0.1", Port: 1}
		if _, err := e.Dial(context.Background(), ep, nil); !errors.Is(err, ErrProtocolUnsupported) {
			t.Fatalf("scheme=%s err=%v", scheme, err)
		}
	}
	if got := observed.Load(); got != 0 {
		t.Fatalf("unsupported operations emitted %d events", got)
	}
}

func TestEpollMethodProtocolMismatch(t *testing.T) {
	e, err := NewEpoll(EpollConfig{Pollers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if _, err := e.Dial(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 1}, nil); !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("stream/udp mismatch err=%v", err)
	}
	if _, err := e.DialPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 1}, nil); !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("packet/tcp mismatch err=%v", err)
	}
}
