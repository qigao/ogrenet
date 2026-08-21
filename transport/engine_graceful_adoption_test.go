package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/qigao/ogrenet"
)

func TestEngineDrainingRejectsNewOperationsAndLateAdoption(t *testing.T) {
	p := dialSessionPair(t, ogrenet.SchemeTCP)
	defer p.close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.clientEngine.Shutdown(ctx) }()

	serverHalf := requireHalfClose(t, p.server)
	waitClosed(t, serverHalf.ReadClosed(), "server read-half while Engine drains")

	snap := p.clientEngine.admissionSnapshot()
	if snap.ActiveConnections != 0 || snap.DrainingConnections != 1 {
		t.Fatalf("admission snapshot while draining = %+v", snap)
	}

	if _, err := p.clientEngine.Dial(context.Background(), p.server.Endpoint(), ogrenet.HandlerFuncs{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Dial while draining = %v, want ErrClosed", err)
	}
	if _, err := p.clientEngine.Listen(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 0}, ogrenet.HandlerFuncs{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Listen while draining = %v, want ErrClosed", err)
	}
	if _, err := p.clientEngine.DialPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 9}, ogrenet.PacketHandlerFuncs{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("DialPacket while draining = %v, want ErrClosed", err)
	}
	if _, err := p.clientEngine.ListenPacket(context.Background(), ogrenet.Endpoint{Scheme: ogrenet.SchemeUDP, Host: "127.0.0.1", Port: 0}, ogrenet.PacketHandlerFuncs{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("ListenPacket while draining = %v, want ErrClosed", err)
	}

	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	if _, err := p.clientEngine.adoptStream(left, ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "late", Port: 1}, ogrenet.HandlerFuncs{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("late adopt while draining = %v, want ErrClosed", err)
	}

	if err := serverHalf.CloseWrite(ctx); err != nil {
		t.Fatalf("server CloseWrite: %v", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Engine.Shutdown = %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
