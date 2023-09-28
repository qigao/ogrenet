package network

import (
	"bufio"
	"context"

	"github.com/rs/zerolog/log"
)

var _ EventHandler = (*DefaultEventHandler)(nil)

type DefaultEventHandler struct{}

func (d *DefaultEventHandler) OnOpen(c Conn) context.Context {
	return context.Background()
}

func (d *DefaultEventHandler) OnClose(ctx context.Context, c Conn) {
	log.Ctx(ctx).Info().Msgf("[OnClose] OgreConn: %d closed", c.Fd())
}

func (d *DefaultEventHandler) OnRead(ctx context.Context, c Conn) {
	data, _, err := bufio.NewReader(c).ReadLine()
	if err != nil {
		log.Error().Err(err).Msgf("err: %v", err)
	}
	log.Info().Msgf("[Handler] read data: %s", string(data))

	if _, err = c.Write([]byte(data)); err != nil {
		log.Error().Err(err).Msgf("err: %v", err)
	}
}
