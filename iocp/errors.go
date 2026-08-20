package iocp

import "errors"

var (
	ErrClosed       = errors.New("iocp: completion port is closed")
	ErrTimeout      = errors.New("iocp: wait timed out")
	ErrReservedKey  = errors.New("iocp: completion key is reserved for internal use")
)
