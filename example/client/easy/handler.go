package main

import (
	"time"

	"github.com/qigao/ogrenet/network"
	"github.com/rs/zerolog/log"
)

// Handler
type Handler struct{}

func (h *Handler) OnConnect(c *network.Conn) {
	log.Info().Msgf("[Handler] remote %v connected", c.RemoteAddr())
}

func (h *Handler) OnMessage(c *network.Conn, bytes []byte) {
	log.Info().Msgf("[Handler] remote %v send message: %s", c.RemoteAddr(), string(bytes))
	c.Write(bytes)
	c.UpdateTime = time.Now().Unix()
}

func (h *Handler) OnClose(c *network.Conn) {
	log.Info().Msgf("[Handler] closed %d", c.Fd())
}
