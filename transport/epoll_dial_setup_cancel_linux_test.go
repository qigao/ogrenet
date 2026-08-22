//go:build linux

package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
)

func TestEpollNativeDialCancellationDuringCodecSetupDoesNotAdoptSession(t *testing.T) {
	_, endpoint, accepted := newNativeDialTarget(t)
	observer := newEpollTestObserver()
	factoryEntered := make(chan struct{})
	factoryRelease := make(chan struct{})
	e := newEpollTestEngine(t, 1,
		WithObserver(observer),
		WithFramerFactory(func() wire.Framer {
			close(factoryEntered)
			<-factoryRelease
			return wire.New(nil)
		}),
	)

	callerCause := errors.New("caller canceled during codec setup")
	ctx, cancel := context.WithCancelCause(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := e.dialNativeTCP(ctx, endpoint, nil, nil)
		result <- err
	}()

	waitTestSignal(t, factoryEntered, "native dial framer factory")
	peer := waitNativeDialAccepted(t, accepted)
	cancel(callerCause)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	select {
	case err := <-result:
		if err != callerCause {
			t.Fatalf("dial error=%v, want exact caller cause %v", err, callerCause)
		}
	case <-waitCtx.Done():
		t.Fatalf("dial did not return while codec setup was blocked: %v", context.Cause(waitCtx))
	}

	close(factoryRelease)
	waitPeerClosed(t, peer)
	event := waitEpollEvent(t, observer, ogrenet.EventConnect)
	if event.ResourceID != 0 || event.Err != nil {
		t.Fatalf("connected-before-cancel event=%+v, want successful transport connect with no adopted ID", event)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	waitTestSignal(t, e.Done(), "engine barrier after codec-setup cancellation")
	stats := e.Stats()
	if stats.OpeningConnections != 0 || stats.ActiveConnections != 0 || stats.DrainingConnections != 0 {
		t.Fatalf("engine stats after codec-setup cancellation=%+v", stats)
	}
}
