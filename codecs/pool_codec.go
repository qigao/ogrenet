package codecs

import (
	"sync"
)

type CodecPool struct {
	HeadPool sync.Pool
	TailPool sync.Pool
}

func NewCodecPool() *CodecPool {
	return &CodecPool{
		HeadPool: sync.Pool{
			New: func() interface{} {
				return new(Head)
			},
		},
		TailPool: sync.Pool{
			New: func() interface{} {
				return new(Tail)
			},
		},
	}
}

func (c *CodecPool) PutCodec(codec *ModBus) {
	c.HeadPool.Put(codec.Head)
	c.TailPool.Put(codec.Tail)
}

func (c *CodecPool) NewEmptyCodecFromPool() ModBus {
	h := c.HeadPool.Get().(*Head)
	t := c.TailPool.Get().(*Tail)
	p := ModBus{
		Head: h,
		Body: ZeroBytes,
		Tail: t,
	}
	return p
}
