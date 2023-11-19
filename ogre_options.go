package ogrenet

import (
	"time"

	"github.com/qigao/ogrenet/codecs"
)

type Option struct {
	TimeOut       TimeOut
	Packet        Packet
	BufSize       BufSize
	KeepAlive     bool
	Mode          WorkMode
	LBOption      LoadBalanceOption
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
	SepType PacketType
	Head    byte
	Tail    byte
}

type BufSize struct {
	PacketSize   int
	ReadBufSize  int
	WriteBufSize int
}

type LoadBalanceOption struct {
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
			Conn:   DefaultConnTimeout,
			Handle: DefaultHandleTimeout,
		},
		BufSize: BufSize{
			PacketSize:   DefaultPacketSize,
			ReadBufSize:  DefaultReadBufSize,
			WriteBufSize: DefaultWriteBufSize,
		},
		Packet: Packet{
			SepType: SepByHeadAndTail,
			Head:    codecs.MagicHead,
			Tail:    codecs.MagicTail,
		},
	}
	return defaultLimiter
}

func SetupLimiterOptions(opts *Option) Limiter {
	limiter := Limiter{}
	if opts == nil {
		return DefaultLimiter()
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
