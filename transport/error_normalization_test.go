package transport

import (
	"context"
	"net"
	"testing"

	"github.com/qigao/ogrenet"
)

func TestStreamAbortPreservesClassifiedClosedCause(t *testing.T) {
	e, err := New()
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

	typed := envelopeOperational(OpWrite, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, net.ErrClosed)
	if !c.abort(abortFailure, typed) {
		t.Fatal("classified failure did not win abort ownership")
	}
	if got := c.Err(); got != typed {
		t.Fatalf("Session.Err = %#v, want classified error identity %#v", got, typed)
	}
	waitClosed(t, c.Done(), "classified-close stream session")
}

func TestListenerClosePreservesClassifiedClosedCause(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &listener{
		endpoint: ogrenet.Endpoint{Scheme: ogrenet.SchemeTCP, Host: "127.0.0.1", Port: 1},
		ln:       raw,
		ctx:      ctx,
		cancel:   cancel,
		closing:  make(chan struct{}),
		done:     make(chan struct{}),
	}

	typed := envelopeOperational(OpAccept, ogrenet.SchemeTCP, raw.Addr(), nil, ErrorUnknown, net.ErrClosed)
	if err := l.initiateClose(typed); err != nil {
		t.Fatal(err)
	}
	if got := l.Err(); got != typed {
		t.Fatalf("Listener.Err = %#v, want classified error identity %#v", got, typed)
	}
}
