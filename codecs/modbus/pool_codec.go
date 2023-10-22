package modbus

import (
	"sync"

	"github.com/qigao/ogrenet/codecs"
)

type ModBusCodecPool struct {
	HeadPool sync.Pool
	TailPool sync.Pool
}

func NewModBusCodecPool() *ModBusCodecPool {
	return &ModBusCodecPool{
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

func (c *ModBusCodecPool) PutCodec(codec *ModBusCodec) {
	c.HeadPool.Put(codec.Head)
	c.TailPool.Put(codec.Tail)
}

func (c *ModBusCodecPool) NewEmptyModBusCodecFromPool() ModBusCodec {
	h := c.HeadPool.Get().(*HeadCodec)
	t := c.TailPool.Get().(*TailCodec)
	p := ModBusCodec{
		Head: h,
		Body: codecs.ZeroBytes,
		Tail: t,
	}
	return p
}
