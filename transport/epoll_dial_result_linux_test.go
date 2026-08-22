//go:build linux

package transport

import (
	"context"
	"testing"
	"time"
)

func TestEpollNativeDialResultPublishesOpeningStateBeforeReturn(t *testing.T) {
	_, endpoint, accepted := newNativeDialTarget(t)
	e := newEpollTestEngine(t, 1)
	r := e.reactors[0]

	installed := make(chan struct{})
	afterResult := make(chan struct{})
	release := make(chan struct{})
	r.signal(newTestInboxItem(func(rr *epollReactor) {
		rr.testAfterDialResult = func(*epollSession) {
			close(afterResult)
			<-release
		}
		close(installed)
	}))
	waitTestSignal(t, installed, "dial-result hook install")

	result := make(chan *epollSession, 1)
	errs := make(chan error, 1)
	go func() {
		session, err := e.dialNativeTCP(context.Background(), endpoint, nil, nil)
		if err != nil {
			errs <- err
			return
		}
		result <- session
	}()
	_ = waitNativeDialAccepted(t, accepted)
	waitTestSignal(t, afterResult, "native dial result publication")

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	var session *epollSession
	select {
	case err := <-errs:
		t.Fatalf("native dial error: %v", err)
	case session = <-result:
	case <-waitCtx.Done():
		t.Fatalf("caller did not receive native Session after result publication: %v", context.Cause(waitCtx))
	}
	if session == nil {
		t.Fatal("native dial returned nil Session")
	}
	if session.state != epollSessionOpening {
		t.Fatalf("published Session state=%v, want Opening", session.state)
	}

	close(release)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	waitTestSignal(t, e.Done(), "engine barrier after result publication test")
}
