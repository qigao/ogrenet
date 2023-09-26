package utils

// BytePool pool for get byte slice
type BytePool struct {
	c      chan []byte
	length int
	ticker *Ticker

	lastSize int
}

// NewBytePool 创建新对象
func NewBytePool(maxSize, length int) *BytePool {
	if maxSize <= 0 {
		maxSize = 1024
	}
	if length <= 0 {
		length = 128
	}
	pool := &BytePool{
		c:      make(chan []byte, maxSize),
		length: length,
	}
	pool.start()
	return pool
}

func (p *BytePool) start() {

}

// Get 获取一个新的byte slice
func (p *BytePool) Get() (b []byte) {
	select {
	case b = <-p.c:
	default:
		b = make([]byte, p.length)
	}
	return
}

// Put 放回一个使用过的byte slice
func (p *BytePool) Put(b []byte) {
	if cap(b) != p.length {
		return
	}
	select {
	case p.c <- b:
	default:
		// 已达最大容量，则抛弃
	}
}

// Size 当前的数量
func (p *BytePool) Size() int {
	return len(p.c)
}

// Destroy 销毁
func (p *BytePool) Destroy() {
	p.ticker.Stop()
}
