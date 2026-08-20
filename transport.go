package ogrenet

import (
	"context"
	"net"
)

// Conn is the platform-independent connection contract exposed to applications.
// Implementations may be backed by epoll, kqueue, or IOCP.
type Conn interface {
	ID() uint64
	Send(ctx context.Context, msg Message) error
	TrySend(msg Message) error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Close() error
}

// Handler receives plaintext application messages after framing and security
// processing have completed.
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
// remain public and keep their native kernel semantics; Engine implementations
// compose them rather than replacing them.
type Engine interface {
	Listen(ctx context.Context, network, address string, h Handler) error
	Dial(ctx context.Context, network, address string) (Conn, error)
	Close() error
}
