package ogrenet

import (
	"time"

	"golang.org/x/sys/unix"
)

const (
	MaxConnTimeout   = 20 * time.Second
	MaxHandleTimeout = 20 * time.Second
	MaxPacketSize    = 1024
	MaxReadBufSize   = 256
	MaxWriteBufSize  = 256
	MaxChanSize      = MaxPacketSize * MaxPacketSize * 2

	EpollListener = unix.EPOLLIN | unix.EPOLLPRI | unix.EPOLLERR | unix.EPOLLHUP | unix.EPOLLET
)
