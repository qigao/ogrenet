package network

import (
	"net"

	"github.com/qigao/ogrenet/utils"
)

type Option func(options *Options)

type Options struct {
	numPoller    int
	listener     func(network, addr string) (net.Listener, error) // Listener for accept conns
	eventHandler EventHandler
	byteBuffer   utils.ByteBuffer
}

func WithNumPoller(num int) Option {
	return func(options *Options) {
		options.numPoller = num
	}
}

func WithListener(fn func(network, addr string) (net.Listener, error)) Option {
	return func(options *Options) {
		options.listener = fn
	}
}

func WithEventHandler(handler EventHandler) Option {
	return func(options *Options) {
		options.eventHandler = handler
	}
}

func WithByteBuffer(byteBuffer utils.ByteBuffer) Option {
	return func(options *Options) {
		options.byteBuffer = byteBuffer
	}
}
