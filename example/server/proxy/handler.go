package main

import (
	"github.com/qigao/ogrenet"

	codec "github.com/qigao/ogrenet/codecs/passthru"
	"github.com/rs/zerolog/log"
)

type Handler struct {
	codecPool *codec.CodecPool
}

func NewProxyHandler(pool *codec.CodecPool) *Handler {
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
	switch codecP.Head.CodecType {
	case codec.Register:
		ack := codec.NewAckCodec(id, codec.RegisterCMD)
		ackBytes, _ := ack.Encode()
		c.PushData(c.Fd(), ackBytes)
	case codec.UnRegister:
		ack := codec.NewAckCodec(id, codec.UnregisterCMD)
		ackBytes, _ := ack.Encode()
		c.Write(ackBytes)
	case codec.HeartBeat:
		ack := codec.NewAckCodec(id, codec.HeartbeatCMD)
		ackBytes, _ := ack.Encode()
		c.Write(ackBytes)
	case codec.Data:
		ack := codec.NewAckCodec(id, codec.DataCMD)
		ackBytes, _ := ack.Encode()
		c.Write(ackBytes)
		h.OnData(c, codecP.GetBody())
	case codec.Close:
		ack := codec.NewAckCodec(id, codec.CloseCMD)
		ackBytes, _ := ack.Encode()
		c.Write(ackBytes)
		h.OnClose(c)
	default:
		log.Fatal().Msgf("Invalid CodecType: %v", codecP.Head.CodecType)
	}
}

func (h *Handler) OnClose(c *ogrenet.Conn) {
	log.Info().Msgf("[Handler] Conn: %d closed", c.Fd())
}
