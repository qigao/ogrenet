package network

import (
	"sync"

	"github.com/qigao/ogrenet/shared/ringbuffer"

	"github.com/qigao/ogrenet/options"
)

type MessagePool struct {
	NetRBuf  *sync.Pool
	BytePool *sync.Pool
	RBuf     *ringbuffer.RingBuffer
}

func NewMessagePool() *MessagePool {
	pool := &MessagePool{}
	pool.NetRBuf = &sync.Pool{New: func() interface{} {
		return make([]byte, options.MaxReadBufSize)
	}}

	pool.BytePool = &sync.Pool{New: func() interface{} {
		return make([]byte, options.MaxWriteBufSize)
	}}
	pool.RBuf = ringbuffer.New(options.MaxPacketSize)
	return pool
}
