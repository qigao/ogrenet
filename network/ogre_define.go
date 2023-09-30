package network

import (
	"context"
	"net"
	"syscall"
)

const (
	ErrEvents = syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP

	ReadEvents = syscall.EPOLLIN | syscall.EPOLLPRI

	WriteEvents = syscall.EPOLLOUT
)

const (
	capacity = 1024 * 4
	NewLine  = "\r\n"
)

type Conn interface {
	net.Conn
	Fd() int
	Flush() error
	Context() context.Context
}

type EventHandler interface {
	OnOpen(c Conn) context.Context
	OnRead(ctx context.Context, c Conn)
	OnClose(ctx context.Context, c Conn)
}
