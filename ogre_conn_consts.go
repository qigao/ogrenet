package ogrenet

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	MaxConnTimeout   = 20 * time.Second
	MaxHandleTimeout = 20 * time.Second
	MaxPacketSize    = 1024
	MaxReadBufSize   = 256
	MaxWriteBufSize  = 256
	EpollListener    = syscall.EPOLLIN | syscall.EPOLLPRI | syscall.EPOLLERR | syscall.EPOLLHUP | unix.EPOLLET
)
