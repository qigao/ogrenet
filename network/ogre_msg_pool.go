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
		return make([]byte, MaxPacketSize)
	}}

	pool.BytePool = &sync.Pool{New: func() interface{} {
		return make([]byte, MaxPacketSize)
	}}
	pool.RBuf = ringbuffer.New(MaxPacketSize * 4)
	return pool
}
