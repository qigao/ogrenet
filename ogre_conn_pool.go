package ogrenet

import (
	"bytes"
	"sync"

	"github.com/qigao/ogrenet/shared/ringbuffer"
)

type MessagePool struct {
	BytePool *sync.Pool
	RBuf     *ringbuffer.RingBuffer
}

func NewMessagePool() *MessagePool {
	pool := &MessagePool{}
	pool.BytePool = &sync.Pool{New: func() interface{} {
		return &bytes.Buffer{}
	}}
	pool.RBuf = ringbuffer.New(MaxPacketSize)
	return pool
}
