package network

import (
	"sync"

	"github.com/qigao/ogrenet/ringbuffer"
)

type MessagePool struct {
	NetRBuf  *sync.Pool
	BytePool *sync.Pool
	RBuf     *ringbuffer.RingBuffer
}

func NewMessagePool() *MessagePool {
	pool := &MessagePool{}
	pool.NetRBuf = &sync.Pool{New: func() interface{} {
		return make([]byte, MaxReadBufSize)
	}}

	pool.BytePool = &sync.Pool{New: func() interface{} {
		return make([]byte, MaxWriteBufSize)
	}}
	pool.RBuf = ringbuffer.New(MaxPacketSize)
	return pool
}
