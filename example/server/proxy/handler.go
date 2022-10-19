package main

import (
	"github.com/qigao/ogrenet/network"
	"github.com/rs/zerolog/log"
)

type Handler struct{}

// OnRegister implements network.ProxyEventHandle.
func (*Handler) OnRegister(c *network.Conn) {
	log.Info().Msgf("[Handler] remote %v registered", c.RemoteAddr())
}

// OnUnRegister implements network.ProxyEventHandle.
func (*Handler) OnUnRegister(c *network.Conn) {
	log.Info().Msgf("[Handler] remote %v unregistered", c.RemoteAddr())
}

func (h *Handler) OnConnect(c *network.Conn) {
	log.Info().Msgf("[Handler] remote %v connected", c.RemoteAddr())
}

func (h *Handler) OnData(c *network.Conn, bytes []byte) {
	log.Info().Msgf("[Handler] remote id:%d, endpoint:%v message: %x", c.Fd(), c.RemoteAddr(), bytes)
	n, err := c.Write(bytes)
	if err != nil {
		log.Error().Err(err).Msgf("write back error: %d, %v", n, err)
		return
	}
}

func (h *Handler) OnClose(c *network.Conn) {
	log.Info().Msgf("[Handler] Conn: %d closed", c.Fd())
}
