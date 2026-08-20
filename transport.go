package ogrenet

import (
	"context"
	"net"
)

// Session is the common connection contract for TCP, TLS, WS, and WSS.
//
// TCP/TLS implementations use byte-stream framing. WS/WSS preserve native
// WebSocket message boundaries. Done closes only after internal I/O work has
// stopped and OnClose has returned. Err is stable once Done is closed.
type Session interface {
	ID() uint64
	Protocol() Scheme
	Endpoint() Endpoint
	Send(ctx context.Context, msg Message) error
	TrySend(msg Message) error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Done() <-chan struct{}
	Err() error
	Close() error
}

// Listener represents one active TCP/TLS/WS/WSS listening endpoint.
type Listener interface {
	Endpoint() Endpoint
	Addr() net.Addr
	Done() <-chan struct{}
	Err() error
	Close() error
}

// Handler receives plaintext application messages. Callbacks for one Session
// are serialized in lifecycle order: OnOpen, zero or more OnMessage calls, then
// OnClose.
type Handler interface {
	OnOpen(Session)
	OnMessage(Session, Message)
	OnClose(Session, error)
}

// HandlerFuncs is a convenience adapter for Handler.
type HandlerFuncs struct {
	Open    func(Session)
	Message func(Session, Message)
	Close   func(Session, error)
}

func (h HandlerFuncs) OnOpen(s Session) {
	if h.Open != nil {
		h.Open(s)
	}
}

func (h HandlerFuncs) OnMessage(s Session, m Message) {
	if h.Message != nil {
		h.Message(s, m)
	}
}

func (h HandlerFuncs) OnClose(s Session, err error) {
	if h.Close != nil {
		h.Close(s, err)
	}
}

// Packet is one UDP datagram payload. Data is owned by the receiver at callback
// boundaries and copied by Send/TrySend before asynchronous transmission.
type Packet struct {
	Data []byte
}

// PacketConn is a UDP socket. DialPacket returns a connected socket with a
// non-nil RemoteAddr and supports Send/TrySend. ListenPacket returns an
// unconnected socket; use SendTo/TrySendTo with the peer address received by
// PacketHandler.
type PacketConn interface {
	Protocol() Scheme
	Endpoint() Endpoint
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Send(ctx context.Context, packet Packet) error
	TrySend(packet Packet) error
	SendTo(ctx context.Context, peer net.Addr, packet Packet) error
	TrySendTo(peer net.Addr, packet Packet) error
	Done() <-chan struct{}
	Err() error
	Close() error
}

// PacketHandler receives UDP datagrams and socket close notification.
type PacketHandler interface {
	OnPacket(PacketConn, net.Addr, Packet)
	OnClose(PacketConn, error)
}

// PacketHandlerFuncs is a convenience adapter for PacketHandler.
type PacketHandlerFuncs struct {
	Packet func(PacketConn, net.Addr, Packet)
	Close  func(PacketConn, error)
}

func (h PacketHandlerFuncs) OnPacket(c PacketConn, peer net.Addr, p Packet) {
	if h.Packet != nil {
		h.Packet(c, peer, p)
	}
}

func (h PacketHandlerFuncs) OnClose(c PacketConn, err error) {
	if h.Close != nil {
		h.Close(c, err)
	}
}

// Engine is the application-facing transport boundary above the native poller
// packages. Session methods accept only TCP/TLS/WS/WSS endpoints; packet methods
// accept only UDP endpoints. There is no protocol fallback or automatic
// downgrade.
//
// Close initiates shutdown and is idempotent. Done closes after every listener,
// session, packet socket, and in-flight Dial/Listen operation has fully
// terminated. Shutdown combines Close with a context-bounded wait for Done.
type Engine interface {
	Listen(ctx context.Context, endpoint Endpoint, h Handler) (Listener, error)
	Dial(ctx context.Context, endpoint Endpoint, h Handler) (Session, error)
	ListenPacket(ctx context.Context, endpoint Endpoint, h PacketHandler) (PacketConn, error)
	DialPacket(ctx context.Context, endpoint Endpoint, h PacketHandler) (PacketConn, error)
	Done() <-chan struct{}
	Shutdown(ctx context.Context) error
	Close() error
}
