package ogrenet

import (
	"context"
	"net"
)

// Conn is the platform-independent connection contract exposed to applications.
// Implementations may be backed by epoll, kqueue, IOCP, or another transport
// implementation that preserves this lifecycle and message contract.
//
// Done closes only after the connection's internal I/O work has stopped and its
// OnClose callback has returned. Err is stable once Done is closed.
type Conn interface {
	ID() uint64
	Send(ctx context.Context, msg Message) error
	TrySend(msg Message) error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Done() <-chan struct{}
	Err() error
	Close() error
}

// Listener represents one active listening endpoint. Done closes after the
// listener's accept loop has stopped and Err is then stable.
type Listener interface {
	Addr() net.Addr
	Done() <-chan struct{}
	Err() error
	Close() error
}

// Handler receives plaintext application messages after framing and security
// processing have completed. Callbacks for one connection are delivered in
// lifecycle order: OnOpen, zero or more OnMessage calls, then OnClose.
type Handler interface {
	OnOpen(Conn)
	OnMessage(Conn, Message)
	OnClose(Conn, error)
}

// HandlerFuncs is a convenience adapter for Handler.
type HandlerFuncs struct {
	Open    func(Conn)
	Message func(Conn, Message)
	Close   func(Conn, error)
}

func (h HandlerFuncs) OnOpen(c Conn) {
	if h.Open != nil {
		h.Open(c)
	}
}

func (h HandlerFuncs) OnMessage(c Conn, m Message) {
	if h.Message != nil {
		h.Message(c, m)
	}
}

func (h HandlerFuncs) OnClose(c Conn, err error) {
	if h.Close != nil {
		h.Close(c, err)
	}
}

// Engine is the common high-level transport contract. Native poller packages
// remain public and retain their kernel-specific semantics; Engine is the
// application-facing message/lifecycle boundary above them.
type Engine interface {
	Listen(ctx context.Context, network, address string, h Handler) (Listener, error)
	Dial(ctx context.Context, network, address string, h Handler) (Conn, error)
	Close() error
}
