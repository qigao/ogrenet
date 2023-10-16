package network

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	DefaultConnTimeout   = 2 * time.Second
	DefaultHandleTimeout = 2 * time.Second
	DefaultBodySize      = 512
	MaxPacketSize        = 512
	EpollListener        = syscall.EPOLLIN | syscall.EPOLLPRI | syscall.EPOLLERR | syscall.EPOLLHUP | unix.EPOLLET
)

type Limiter struct {
	Timeout Timeout
	Packet  Packet
}

type Timeout struct {
	conn   time.Duration
	handle time.Duration
}

type Packet struct {
	SepType    PacketType
	Head       byte
	Tail       byte
	PacketSize int
}
