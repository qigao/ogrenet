//go:build linux

package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/qigao/ogrenet"
	"golang.org/x/sys/unix"
)

func TestEpollNativeDialCancellationAfterRegistrationReturnsExactCauseWithoutCallerClose(t *testing.T) {
	_, endpoint, accepted := newNativeDialTarget(t)
	observer := newEpollTestObserver()
	e := newEpollTestEngine(t, 1, WithObserver(observer))
	r := e.reactors[0]

	installed := make(chan struct{})
	registered := make(chan struct{})
	inspectFD := make(chan struct{})
	fdCheck := make(chan error, 1)
	r.signal(newTestInboxItem(func(rr *epollReactor) {
		rr.testAfterConnectRegistered = func(s *epollSession) {
			close(registered)
			<-inspectFD
			_, err := unix.FcntlInt(uintptr(s.fd), unix.F_GETFD, 0)
			fdCheck <- err
		}
		close(installed)
	}))
	waitTestSignal(t, installed, "connect-registration hook install")

	callerCause := errors.New("caller canceled after connect registration")
	ctx, cancel := context.WithCancelCause(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := e.dialNativeTCP(ctx, endpoint, nil, nil)
		result <- err
	}()
	waitTestSignal(t, registered, "native connect Poller.Add")
	cancel(callerCause)

	select {
	case err := <-result:
		if err != callerCause {
			t.Fatalf("dial error=%v, want exact caller cause %v", err, callerCause)
		}
	case <-ctx.Done():
		// ctx cancellation is expected, but the dial goroutine still must publish
		// its exact result promptly while the reactor is blocked in the hook.
		select {
		case err := <-result:
			if err != callerCause {
				t.Fatalf("dial error=%v, want exact caller cause %v", err, callerCause)
			}
		default:
			t.Fatal("dial did not return after caller cancellation")
		}
	}

	close(inspectFD)
	if err := <-fdCheck; err != nil {
		t.Fatalf("reactor observed caller-closed fd: %v", err)
	}
	peer := waitNativeDialAccepted(t, accepted)
	waitPeerClosed(t, peer)

	event := waitEpollEvent(t, observer, ogrenet.EventConnect)
	if event.ResourceID != 0 || event.Err != callerCause {
		t.Fatalf("canceled connect event=%+v, want exact caller cause", event)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	waitTestSignal(t, e.Done(), "engine barrier after canceled native dial")
}
