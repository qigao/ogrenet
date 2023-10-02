package main

import (
	"bufio"
	"context"

	"github.com/qigao/ogrenet/network"
	"github.com/rs/zerolog/log"
)

// var _ network.EventHandler = (*Handler)(nil)
type Handler struct{}

func (h *Handler) OnOpen(c network.Conn) context.Context {
	log.Info().Msgf("[Handler] remote %v connected", c.RemoteAddr())
	return context.WithValue(context.Background(), CtxKey, Message{Msg: "helloword"})
}

func (h *Handler) OnRead(ctx context.Context, c network.Conn) {
	_, ok := ctx.Value(CtxKey).(Message)
	if !ok {
		return
	}
	data, _, err := bufio.NewReader(c).ReadLine()
	if err != nil {
		log.Error().Err(err).Msgf("err: %v", err)
	}
	log.Info().Msgf("[Handler] read data: %s", string(data))

	if _, err = c.Write([]byte(data)); err != nil {
		log.Error().Err(err).Msgf("err: %v", err)
	}
}

func (h *Handler) OnClose(_ context.Context, c network.Conn) {
	log.Info().Msgf("[Handler] closed %d", c.Fd())
}
