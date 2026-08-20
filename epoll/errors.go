package epoll

import "errors"

var (
	ErrClosed         = errors.New("epoll: poller is closed")
	ErrConcurrentWait = errors.New("epoll: concurrent Wait calls are not supported")
	ErrNoEvents       = errors.New("epoll: destination event slice is empty")
	ErrReservedData   = errors.New("epoll: data value is reserved for internal use")
)
