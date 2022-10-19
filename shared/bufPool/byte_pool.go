package bufPool

import "sync"

type BytePool struct {
	p sync.Pool
}

func NewBytePool(size, cap int) *BytePool {
	if size > cap {
		panic("size must be less then cap")
	}
	p := &BytePool{}
	p.p.New = func() any {
		return make([]byte, size, cap)
	}
	return p
}

func (p *BytePool) Get() []byte {
	return p.p.Get().([]byte)
}

func (p *BytePool) Put(b []byte) {
	b = b[:0]
	p.p.Put(b)
}
