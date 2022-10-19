package passthru

import (
	"sync"
)

type CodecPool struct {
	Passthru *sync.Pool
}

func NewCodecPool() *CodecPool {
	return &CodecPool{
		Passthru: &sync.Pool{
			New: func() interface{} {
				return NewEmptyPassThruCodec()
			},
		},
	}
}
