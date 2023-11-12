package bufPool

import (
	"sync"
	"testing"
)

func BenchmarkMakeStack(b *testing.B) {
	for N := 0; N < b.N; N++ {
		obj := make([]byte, 1024)
		_ = obj
	}
}

var obj []byte

func BenchmarkMakeHeap(b *testing.B) {
	for N := 0; N < b.N; N++ {
		obj = make([]byte, 1024)
		_ = obj
	}
}

var bytePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 1024)
		return &b
	},
}

func BenchmarkBytePoolPointer(b *testing.B) {
	for N := 0; N < b.N; N++ {
		obj := bytePool.Get().(*[]byte)
		_ = obj
		bytePool.Put(obj)
	}
}
