package network

import (
	"context"
	"syscall"

	"github.com/rs/zerolog/log"
)

const (
	ErrEvents = syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP

	ReadEvents = syscall.EPOLLIN | syscall.EPOLLPRI

	WriteEvents = syscall.EPOLLOUT
)

type EventHandler interface {
	OnOpen(c Conn) context.Context
	OnRead(ctx context.Context, c Conn)
	OnClose(ctx context.Context, c Conn)
}

var _ EventHandler = (*eventHandler)(nil)

type eventHandler struct{}

func (d *eventHandler) OnOpen(c Conn) context.Context {
	return context.Background()
}

func (d *eventHandler) OnClose(ctx context.Context, c Conn) {
	log.Ctx(ctx).Info().Msgf("[OnClose] conn: %d closed", c.Fd())
}

func (d *eventHandler) OnRead(ctx context.Context, c Conn) {
	// todo set reader buffer
	b := make([]byte, 1024)
	n, err := c.Read(b[:cap(b)])
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msgf("[OnRead] conn: %d read error", c.Fd())
	}
	log.Ctx(ctx).Info().Msgf("read data: %s", string(b[:n]))
	n, err = c.Write(b[:n])
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msgf("[OnRead] conn: %d write error", c.Fd())
	}
	log.Ctx(ctx).Info().Msgf("write data: %s", string(b[:n]))
}
