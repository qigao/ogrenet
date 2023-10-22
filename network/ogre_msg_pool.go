package network

import (
	"bytes"
	"sync"

	"github.com/qigao/ogrenet/shared/ringbuffer"

	"github.com/qigao/ogrenet/options"
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
	pool.RBuf = ringbuffer.New(options.MaxPacketSize)
	return pool
}
