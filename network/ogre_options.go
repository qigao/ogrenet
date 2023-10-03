package network

import "github.com/qigao/ogrenet/codecs"

type Option func(options *Options)

type Options struct {
	ReadBufferSize    int
	WriteBufferSize   int
	ConnectionTimeOut int64
	CompressLevel     int
	IsCompressOn      bool
	numPoller         int
	messageSep        []byte
	eventHandle       EventHandle
	Codec             codecs.Codec
}

func WithNumPoller(num int) Option {
	return func(options *Options) {
		options.numPoller = num
	}
}

func WithEventHandler(handle EventHandle) Option {
	return func(options *Options) {
		options.eventHandle = handle
	}
}

func WithMessageSeparator(sep []byte) Option {
	return func(options *Options) {
		options.messageSep = make([]byte, len(sep))
		copy(options.messageSep, sep)
	}
}

func WithMessageCodec(codec codecs.Codec) Option {
	return func(options *Options) {
		options.Codec = codec
	}
}
