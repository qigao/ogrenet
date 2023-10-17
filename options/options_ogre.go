package options

import "time"

type Options struct {
	TimeOut   TimeOut
	Packet    Packet
	BufSize   BufSize
	ProxyAlgo AlgoType
	KeepAlive bool

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
