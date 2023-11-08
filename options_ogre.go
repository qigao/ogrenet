package ogrenet

import (
	"time"
)

type Options struct {
	TimeOut       TimeOut
	Packet        Packet
	BufSize       BufSize
	KeepAlive     bool
	proxyMode     ProxyMode
	rotateCfg     RotateOptions
	CompressLevel int
	numPoller     int
}

type Limiter struct {
	Timeout TimeOut
	Packet  Packet
	BufSize BufSize
}

type TimeOut struct {
	Conn   time.Duration
	Handle time.Duration
}

type Packet struct {
	CutType PacketType
	Head    byte
	Tail    byte
}

type BufSize struct {
	PacketSize   int
	ReadBufSize  int
	WriteBufSize int
}

type RotateOptions struct {
	// Keys are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big PartitionCount if you have
	// too many keys.
	PartitionCount int

	// Members are replicated on consistent hash ring. This number means that a member
	// how many times replicated on the ring.
	ReplicationFactor int

	// Load is used to calculate average load. See the code, the paper and Google's blog post to learn about it.
	Load float64
}

func DefaultLimiter() Limiter {
	defaultLimiter := Limiter{
		Timeout: TimeOut{
			Conn:   MaxConnTimeout,
			Handle: MaxHandleTimeout,
		},
		BufSize: BufSize{
			PacketSize:   MaxPacketSize,
			ReadBufSize:  MaxReadBufSize,
			WriteBufSize: MaxWriteBufSize,
		},
	}
	return defaultLimiter
}

func SetupLimiterOptions(opts *Options) Limiter {
	limiter := Limiter{}
	if opts == nil {
		return limiter
	}
	if opts.TimeOut != (TimeOut{}) {
		limiter.Timeout = opts.TimeOut
	}
	if opts.BufSize != (BufSize{}) {
		limiter.BufSize = opts.BufSize
	}
	if opts.Packet != (Packet{}) {
		limiter.Packet = opts.Packet
	}
	return limiter
}
