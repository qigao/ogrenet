package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/wire"
)

func TestTCPReadIdleExcludesHandlerTime(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{ReadIdle: 60 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	defer right.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{
		Message: func(ogrenet.Session, ogrenet.Message) {
			close(entered)
			<-release
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := wire.New(nil).Encode(ogrenet.Text("handler"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = right.Write(frame) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler was not entered")
	}
	select {
	case <-c.Done():
		t.Fatalf("ReadIdle counted handler execution: %v", c.Err())
	case <-time.After(140 * time.Millisecond):
	}
	close(release)
	waitSessionTimeoutKind(t, c, TimeoutReadIdle)
}

func TestTCPReadIdleRefreshesOnPartialProgress(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{ReadIdle: 200 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	defer right.Close()
	message := make(chan struct{})
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{
		Message: func(ogrenet.Session, ogrenet.Message) { close(message) },
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := wire.New(nil).Encode(ogrenet.Text("partial-progress"))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for offset := 0; offset < len(frame); {
			end := offset + 4
			if end > len(frame) {
				end = len(frame)
			}
			_, _ = right.Write(frame[offset:end])
			offset = end
			if offset < len(frame) {
				time.Sleep(70 * time.Millisecond)
			}
		}
	}()
	select {
	case <-message:
	case <-c.Done():
		t.Fatalf("session timed out while partial reads were progressing: %v", c.Err())
	case <-time.After(2 * time.Second):
		t.Fatal("message was not reconstructed from partial reads")
	}
	waitSessionTimeoutKind(t, c, TimeoutReadIdle)
}

func TestExplicitCloseBeforeTimeoutKeepsNilCause(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{MaxLifetime: 80 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	defer right.Close()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not close")
	}
	time.Sleep(120 * time.Millisecond)
	if c.Err() != nil {
		t.Fatalf("late timeout overwrote explicit close: %v", c.Err())
	}
}

func TestTimeoutCauseSurvivesLaterClose(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{MaxLifetime: 50 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	defer right.Close()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	waitSessionTimeoutKind(t, c, TimeoutMaxLifetime)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	var te *TimeoutError
	if !errors.As(c.Err(), &te) || te.Kind != TimeoutMaxLifetime {
		t.Fatalf("late Close overwrote timeout cause: %#v", c.Err())
	}
}

func TestWriteTimeoutReleasesEngineAccounting(t *testing.T) {
	e, err := New(
		WithLimits(Limits{MaxConnections: 1, MaxQueuedBytesTotal: 1 << 20}),
		WithTimeouts(Timeouts{Write: 40 * time.Millisecond}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	left, right := net.Pipe()
	defer right.Close()
	c, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Send(ctx, ogrenet.Bin([]byte("blocked")))
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not finish after write timeout")
	}
	waitAdmissionZero(t, e)
}

func TestEngineDoneIncludesActivityWatchdog(t *testing.T) {
	e, err := New(WithTimeouts(Timeouts{ConnectionIdle: time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	defer right.Close()
	if _, err := e.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "pipe", Port: 1}, ogrenet.HandlerFuncs{}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Done():
	case <-time.After(time.Second):
		t.Fatal("Engine.Done did not wait for watchdog shutdown")
	}
	waitAdmissionZero(t, e)
}

func waitAdmissionZero(t *testing.T, e *Engine) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		snap := e.admissionSnapshot()
		if snap.OpeningConnections == 0 && snap.ActiveConnections == 0 && snap.ActiveHandshakes == 0 && snap.PendingUpgrades == 0 && snap.GlobalQueuedBytes == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime accounting did not return to zero: %+v", snap)
		}
		time.Sleep(time.Millisecond)
	}
}
