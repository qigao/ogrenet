package ogrenet

import (
	"time"

	"golang.org/x/sys/unix"
)

const (
	DefaultConnTimeout   = 20 * time.Second
	DefaultHandleTimeout = 20 * time.Second
	DefaultPacketSize    = 1024
	DefaultReadBufSize   = 256
	DefaultWriteBufSize  = 256
	DefaultChanSize      = DefaultPacketSize * DefaultPacketSize * 2

	EpollListener = unix.EPOLLIN | unix.EPOLLPRI | unix.EPOLLERR | unix.EPOLLHUP | unix.EPOLLET
)
