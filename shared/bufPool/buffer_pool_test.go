package bufPool

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"
	"testing"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func GetBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

func PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

type TestingData struct {
	Data string `json:"data"`
	Key  string `json:"key"`
}

func BenchmarkReadStreamWithPool(b *testing.B) {
	data := TestingData{
		Data: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque molestie.",
		Key:  "Lorem",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf := GetBuffer()

		_ = json.NewEncoder(buf).Encode(&data)
		io.Copy(io.Discard, buf)

		PutBuffer(buf)
	}
}

func BenchmarkReadStreamWithoutPool(b *testing.B) {
	data := TestingData{
		Data: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque molestie.",
		Key:  "Lorem",
	}
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		buf := new(bytes.Buffer)
		_ = json.NewEncoder(buf).Encode(&data)
		io.Copy(io.Discard, buf)
	}
}

var bPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

var data = make([]byte, 10000)

func BenchmarkBufferWithPool(b *testing.B) {
	for n := 0; n < b.N; n++ {
		buf := bPool.Get().(*bytes.Buffer)
		buf.Write(data)
		buf.Reset()
		bPool.Put(buf)
	}
}

func BenchmarkBuffer(b *testing.B) {
	for n := 0; n < b.N; n++ {
		var buf bytes.Buffer
		buf.Write(data)
	}
}
