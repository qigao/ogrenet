package bufPool

import "testing"

const (
	blocks    = 64
	blockSize = 1024
)

var block = make([]byte, blockSize)

func BenchmarkByte(b *testing.B) {
	for n := 0; n < b.N; n++ {
		// 从长度为0的字节数组开始
		var b []byte
		for i := 0; i < blocks; i++ {
			b = append(b, block...)
		}
	}
}

func BenchmarkMake(b *testing.B) {
	for n := 0; n < b.N; n++ {
		// 预先保留需要的空间
		b := make([]byte, 0, blocks*blockSize)
		for i := 0; i < blocks; i++ {
			b = append(b, block...)
		}
	}
}

func BenchmarkBytePool(b *testing.B) {
	pool := NewBytePool(0, blocks*blockSize)
	for n := 0; n < b.N; n++ {
		// 拿字节数组
		b := pool.Get()
		for i := 0; i < blocks; i++ {
			b = append(b, block...)
		}
		// 归还
		pool.Put(b)
	}
}
