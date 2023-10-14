package network

import "github.com/qigao/ogrenet/codecs"

type Options struct {
	ReadBufferSize  int
	WriteBufferSize int
	ConnTimeOut     int64
	CompressLevel   int
	numPoller       int
	messageSep      []byte
	EventHandle     EventHandle
	Codec           codecs.Codec
}
