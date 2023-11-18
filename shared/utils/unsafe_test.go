package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString2ByteSlice(t *testing.T) {
	t.Run("non-empty string", func(t *testing.T) {
		str := "Hello, World!"
		expected := []byte{72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33}
		result := String2ByteSlice(str)
		assert.Equal(t, expected, result)
	})

	t.Run("empty string", func(t *testing.T) {
		str := ""
		expected := []byte(nil)
		result := String2ByteSlice(str)
		assert.Equal(t, expected, result)
	})
}

func TestByteSlice2String(t *testing.T) {
	t.Run("non-empty byte slice", func(t *testing.T) {
		bs := []byte{72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33}
		expected := "Hello, World!"
		result := ByteSlice2String(bs)
		assert.Equal(t, expected, result)
	})

	t.Run("empty byte slice", func(t *testing.T) {
		bs := []byte{}
		expected := ""
		result := ByteSlice2String(bs)
		assert.Equal(t, expected, result)
	})
}

func BenchmarkString2ByteSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		String2ByteSlice("Hello, World!")
	}
}

func BenchmarkByteSlice2String(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ByteSlice2String([]byte{72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33})
	}
}

func BenchmarkString2ByteSliceEmpty(b *testing.B) {
	for i := 0; i < b.N; i++ {
		String2ByteSlice("")
	}
}

func BenchmarkByteSlice2StringEmpty(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ByteSlice2String([]byte{})
	}
}

func BenchmarkString2ByteSliceParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			String2ByteSlice("Hello, World!")
		}
	})
}

func BenchmarkByteSlice2StringParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ByteSlice2String([]byte{72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33})
		}
	})
}

func BenchmarkSafeString2ByteSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = []byte("Hello, World!")
	}
}

func BenchmarkSafeByteSlice2String(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = string([]byte{72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33})
	}
}

func BenchmarkSafeString2ByteSliceEmpty(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = []byte("")
	}
}

func BenchmarkSafeByteSlice2StringEmpty(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = string([]byte{})
	}
}
