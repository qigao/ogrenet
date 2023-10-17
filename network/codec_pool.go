package network

import (
	"sync"

	"github.com/qigao/ogrenet/codecs/passthru"
)

type CodecPool struct {
	passthru *sync.Pool
}

func NewCodecPool() *CodecPool {
	return &CodecPool{
		passthru: &sync.Pool{
			New: func() interface{} {
				return passthru.NewEmptyPassThruCodec()
			},
		},
	}
}
