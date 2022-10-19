package passthru

import (
	"sync"

	"github.com/qigao/ogrenet/codecs"
	"github.com/qigao/ogrenet/options"
	"github.com/qigao/ogrenet/shared/crc16"
)

type CodecPool struct {
	HeadPool sync.Pool
	TailPool sync.Pool
}

func NewCodecPool() *CodecPool {
	return &CodecPool{
		HeadPool: sync.Pool{
			New: func() interface{} {
				return new(HeadCodec)
			},
		},
		TailPool: sync.Pool{
			New: func() interface{} {
				return new(TailCodec)
			},
		},
	}
}

func (c *CodecPool) PutCodec(codec *CodecPassThru) {
	c.HeadPool.Put(codec.Head)
	c.TailPool.Put(codec.Tail)
}

func (c *CodecPool) NewEmptyPassThruCodecFromPool() CodecPassThru {
	h := c.HeadPool.Get().(*HeadCodec)
	t := c.TailPool.Get().(*TailCodec)
	p := CodecPassThru{
		Head: h,
		Body: codecs.ZeroBytes,
		Tail: t,
	}
	return p
}

func (c *CodecPool) NewRegisterCodec(id [4]byte, cseq [4]byte) CodecPassThru {
	h := c.HeadPool.Get().(*HeadCodec)
	t := c.TailPool.Get().(*TailCodec)

	h.Magic = options.DefaultMagicHead
	h.Version = 0
	h.CodecType = Register
	h.ID = id
	h.BodyLen = 1
	h.Cseq = cseq
	t.Magic = options.DefaultMagicTail
	t.CRC = codecs.ZeroCRC16
	return CodecPassThru{
		Head: h,
		Body: codecs.ZeroBytes,
		Tail: t,
	}
}

func (c *CodecPool) NewUnRegisterCodec(id [4]byte, cseq [4]byte) CodecPassThru {
	h := c.HeadPool.Get().(*HeadCodec)
	t := c.TailPool.Get().(*TailCodec)

	h.Magic = options.DefaultMagicHead
	h.Version = 0
	h.CodecType = UnRegister
	h.ID = id
	h.BodyLen = 1
	h.Cseq = cseq

	t.Magic = options.DefaultMagicTail
	t.CRC = codecs.ZeroCRC16

	return CodecPassThru{
		Head: h,
		Body: codecs.ZeroBytes,
		Tail: t,
	}
}

func (c *CodecPool) NewAckCodec(id [4]byte, cseq [4]byte, data []byte) CodecPassThru {
	h := c.HeadPool.Get().(*HeadCodec)
	t := c.TailPool.Get().(*TailCodec)

	h.Magic = options.DefaultMagicHead
	h.Version = 0
	h.CodecType = Ack
	h.ID = id
	h.BodyLen = uint16(len(data))
	h.Cseq = cseq

	t.Magic = options.DefaultMagicTail
	t.CRC = codecs.ZeroCRC16

	return CodecPassThru{
		Head: h,
		Body: data,
		Tail: t,
	}
}

func (c *CodecPool) NewHeartBeatCodec(id [4]byte, cseq [4]byte) CodecPassThru {
	h := c.HeadPool.Get().(*HeadCodec)
	t := c.TailPool.Get().(*TailCodec)

	h.Magic = options.DefaultMagicHead
	h.Version = 0
	h.CodecType = HeartBeat
	h.ID = id
	h.BodyLen = 1
	h.Cseq = cseq

	t.Magic = options.DefaultMagicTail
	t.CRC = codecs.ZeroCRC16

	return CodecPassThru{
		Head: h,
		Body: codecs.ZeroBytes,
		Tail: t,
	}
}

func (c *CodecPool) NewDataCodec(id [4]byte, cseq [4]byte, data []byte) CodecPassThru {
	h := c.HeadPool.Get().(*HeadCodec)
	t := c.TailPool.Get().(*TailCodec)

	h.Magic = options.DefaultMagicHead
	h.Version = 0
	h.CodecType = Data
	h.ID = id
	h.BodyLen = uint16(len(data))
	h.Cseq = cseq

	t.Magic = options.DefaultMagicTail
	t.CRC = crc16.CheckSum(data)
	return CodecPassThru{
		Head: h,
		Body: data,
		Tail: t,
	}
}

func (c *CodecPool) NewCloseCodec(id [4]byte, cseq [4]byte) CodecPassThru {
	h := c.HeadPool.Get().(*HeadCodec)
	t := c.TailPool.Get().(*TailCodec)

	h.Magic = options.DefaultMagicHead
	h.Version = 0
	h.CodecType = Close
	h.ID = id
	h.BodyLen = 1
	h.Cseq = cseq

	t.Magic = options.DefaultMagicTail
	t.CRC = codecs.ZeroCRC16

	return CodecPassThru{
		Head: h,
		Body: codecs.ZeroBytes,
		Tail: t,
	}
}

func (c *CodecPool) NewReConnectCodec(id [4]byte) CodecPassThru {
	h := c.HeadPool.Get().(*HeadCodec)
	t := c.TailPool.Get().(*TailCodec)

	h.Magic = options.DefaultMagicHead
	h.Version = 0
	h.CodecType = ReConnect
	h.ID = id
	h.BodyLen = 1
	h.Cseq = codecs.Empty

	t.Magic = options.DefaultMagicTail
	t.CRC = codecs.ZeroCRC16

	return CodecPassThru{
		Head: h,
		Body: codecs.ZeroBytes,
		Tail: t,
	}
}
