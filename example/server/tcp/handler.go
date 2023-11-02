package main

import (
	"github.com/qigao/ogrenet"
	"github.com/rs/zerolog/log"
)

type Handler struct{}

func (h *Handler) OnConnect(c *ogrenet.Conn) {
	log.Info().Msgf("[Handler] remote %v connected", c.RemoteAddr())
}

func (h *Handler) OnData(c *ogrenet.Conn, bytes []byte) {
	log.Info().Msgf("[Handler] remote id:%d, endpoint:%v message: %x", c.Fd(), c.RemoteAddr(), bytes)
	n, err := c.Write(bytes)
	if err != nil {
		log.Error().Err(err).Msgf("write back error: %d, %v", n, err)
		return
	}
}

func (h *Handler) OnClose(c *ogrenet.Conn) {
	log.Info().Msgf("[Handler] Conn: %d closed", c.Fd())
}
