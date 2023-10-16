package network

import "github.com/qigao/ogrenet/codecs"

type Options struct {
	TimeOut       TimeOut
	Packet        Packet
	BufSize       BufSize
	CompressLevel int
	numPoller     int
	EventHandle   EventHandle
	Codec         codecs.Codec
}
