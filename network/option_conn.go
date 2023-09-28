package network

import (
	"net"
)

type Option func(options *Options)

type Options struct {
	numPoller    int
	listener     func(network, addr string) (net.Listener, error) // NetListener for accept conns
	eventHandler EventHandler
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
