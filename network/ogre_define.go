package network

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	MaxConnTimeout   = 20 * time.Second
	MaxHandleTimeout = 20 * time.Second
	MaxPacketSize    = 512
	MaxReadBufSize   = 1024
	MaxWriteBufSize  = 1024
	EpollListener    = syscall.EPOLLIN | syscall.EPOLLPRI | syscall.EPOLLERR | syscall.EPOLLHUP | unix.EPOLLET
)

type Limiter struct {
	Timeout TimeOut
	Packet  Packet
	BufSize BufSize
}

type TimeOut struct {
	conn   time.Duration
	handle time.Duration
}

type Packet struct {
	SepType PacketType
	Head    byte
	Tail    byte
}

type BufSize struct {
	PacketSize   int
	ReadBufSize  int
	WriteBufSize int
}
