package bufPool

type ByteFixPool struct {
	cache chan []byte
	size  int
	cap   int
}

// cacheSize: 字节池缓存长度
// size: 字节数组长度
// cap: 字节数组容量
func NewByteFixPool(cacheSize, size, cap int) *ByteFixPool {
	if size > cap {
		panic("size must be less then cap")
	}
	return &ByteFixPool{
		cache: make(chan []byte, cacheSize),
		size:  size,
		cap:   cap,
	}
}

func (p *ByteFixPool) Get() []byte {
	select {
	// 从channel读
	case b := <-p.cache:
		return b
		// 如果channel空则申请一个新的字节数组
	default:
		return make([]byte, p.size, p.cap)
	}
}

func (p *ByteFixPool) Put(b []byte) {
	// 重置已用大小
	b = b[:0]
	select {
	// 放入channel
	case p.cache <- b:
	// channel满了则丢弃字节数组
	default:
	}
}
