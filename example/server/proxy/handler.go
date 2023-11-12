package main

import (
	"github.com/qigao/ogrenet"

	. "github.com/qigao/ogrenet/codecs/passthru"
	"github.com/rs/zerolog/log"
)

type Handler struct {
	codecPool *CodecPool
}

func NewProxyHandler(pool *CodecPool) *Handler {
	return &Handler{
		codecPool: pool,
	}
}

func (h *Handler) OnConnect(c *ogrenet.Conn) {
	log.Info().Msgf("[Handler] remote %v connected", c.RemoteAddr())
}

func (h *Handler) OnData(c *ogrenet.Conn, bytes []byte) {
	log.Info().Msgf("[Handler] remote id:%d, endpoint:%v message: %x", c.Fd(), c.RemoteAddr(), bytes)
	codecP := h.codecPool.NewEmptyPassThruCodecFromPool()
	codecP.Decode(bytes)
	id := codecP.Head.ID
	codecType := codecP.Head.CMD
	switch codecType {
	case Register:
		ack := NewAckCodec(id, RegisterCMD)
		ackBytes, _ := ack.Encode()
		c.Write(ackBytes)
	case UnRegister:
		ack := NewAckCodec(id, UnregisterCMD)
		ackBytes, _ := ack.Encode()
		c.Write(ackBytes)
	case HeartBeat:
		ack := NewAckCodec(id, HeartbeatCMD)
		ackBytes, _ := ack.Encode()
		c.Write(ackBytes)
	case Data:
		ack := NewAckCodec(id, DataCMD)
		ackBytes, _ := ack.Encode()
		c.Write(ackBytes)
	case Close:
		ack := NewAckCodec(id, CloseCMD)
		ackBytes, _ := ack.Encode()
		c.Write(ackBytes)
	default:
		log.Debug().Msgf("Invalid CodecType: %v", codecType)
	}
}

func (h *Handler) OnClose(c *ogrenet.Conn) {
	log.Info().Msgf("[Handler] Conn: %d closed", c.Fd())
}
