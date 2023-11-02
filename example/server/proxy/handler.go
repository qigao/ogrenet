package main

import (
	"github.com/qigao/ogrenet"
	"github.com/rs/zerolog/log"
)

type Handler struct{}

// OnRegister implements network.ProxyEventHandle.
func (*Handler) OnRegister(c *ogrenet.Conn) {
	log.Info().Msgf("[Handler] remote %v registered", c.RemoteAddr())
}

// OnUnRegister implements network.ProxyEventHandle.
func (*Handler) OnUnRegister(c *ogrenet.Conn) {
	log.Info().Msgf("[Handler] remote %v unregistered", c.RemoteAddr())
}

func (h *Handler) OnConnect(c *ogrenet.Conn) {
	log.Info().Msgf("[Handler] remote %v connected", c.RemoteAddr())
}

func (h *Handler) OnData(c *ogrenet.Conn, bytes []byte) {
	log.Info().Msgf("[Handler] remote id:%d, endpoint:%v message: %x", c.Fd(), c.RemoteAddr(), bytes)
}

func (h *Handler) OnClose(c *ogrenet.Conn) {
	log.Info().Msgf("[Handler] Conn: %d closed", c.Fd())
}
