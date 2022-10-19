package network

import (
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	MaxPacketSize = 128
	EpollListener = syscall.EPOLLIN | syscall.EPOLLPRI | syscall.EPOLLERR | syscall.EPOLLHUP | unix.EPOLLET
)
