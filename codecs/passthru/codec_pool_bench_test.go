package passthru

import (
	"testing"

	"github.com/qigao/ogrenet/codecs"
)

func BenchmarkEmptyPassThruWithoutPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewEmptyPassThruCodec()
	}
}

func BenchmarkCodecPool(b *testing.B) {
	pool := NewCodecPool()
	for i := 0; i < b.N; i++ {
		h := pool.HeadPool.Get().(*HeadCodec)
		t := pool.TailPool.Get().(*TailCodec)
		c := CodecPassThru{
			Head: h,
			Body: codecs.ZeroBytes,
			Tail: t,
		}
		pool.PutCodec(&c)
	}
}

func BenchmarkEmptyPassThruWithCodecPool(b *testing.B) {
	pool := NewCodecPool()
	for i := 0; i < b.N; i++ {
		c := pool.NewEmptyPassThruCodecFromPool()
		pool.PutCodec(&c)
	}
}

func BenchmarkRegisterCodec(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewRegisterCodec(id)
	}
}

func BenchmarkRegisterCodecPool(b *testing.B) {
	pool := NewCodecPool()
	for i := 0; i < b.N; i++ {
		c := pool.NewRegisterCodec(id)
		pool.PutCodec(&c)

	}
}

func BenchmarkRegisterCodecWithBodyPool(b *testing.B) {
	pool := NewCodecPool()
	data := []byte{0x01, 0x02, 0x03}
	for i := 0; i < b.N; i++ {
		c := pool.NewRegisterWithBody(id, data)
		pool.PutCodec(&c)

	}
}

func BenchmarkDataCodec(b *testing.B) {
	data := []byte{0x01, 0x02, 0x03}
	for i := 0; i < b.N; i++ {
		NewDataCodec(id, data)
	}
}

func BenchmarkDataCodecFromPool(b *testing.B) {
	data := []byte{0x01, 0x02, 0x03}
	pool := NewCodecPool()
	for i := 0; i < b.N; i++ {
		c := pool.NewDataCodec(id, data)
		pool.PutCodec(&c)
	}
}
