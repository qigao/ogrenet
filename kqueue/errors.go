package kqueue

import "errors"

var (
	ErrClosed         = errors.New("kqueue: poller is closed")
	ErrNoEvents       = errors.New("kqueue: event buffer is empty")
	ErrConcurrentWait = errors.New("kqueue: concurrent Wait is not supported")
	ErrReservedEvent  = errors.New("kqueue: event identity is reserved")
)
