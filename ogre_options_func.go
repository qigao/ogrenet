package ogrenet

import "time"

type OptionFunc func(*Option)

func WithPacket(p PacketType, h byte, t byte) OptionFunc {
	return func(o *Option) {
		o.Packet.SepType = p
		o.Packet.Head = h
		o.Packet.Tail = t
	}
}

func WithTimeOut(conn time.Duration, handle time.Duration) OptionFunc {
	return func(o *Option) {
		if conn > 0 && handle > 0 {
			o.TimeOut.Conn = conn
			o.TimeOut.Handle = handle
		}
	}
}

func WithBufSize(packetSize int, readBufSize int, writeBufSize int) OptionFunc {
	return func(o *Option) {
		if packetSize > 0 && readBufSize > 0 && writeBufSize > 0 {
			o.BufSize.PacketSize = packetSize
			o.BufSize.ReadBufSize = readBufSize
			o.BufSize.WriteBufSize = writeBufSize
		}
	}
}

func WithLoadBalanceOptions(partitionCount int, replicationFactor int, load float64) OptionFunc {
	return func(o *Option) {
		if partitionCount > 1 && replicationFactor > 0 && load > 0 {
			o.LBOption.PartitionCount = partitionCount
			o.LBOption.ReplicationFactor = replicationFactor
			o.LBOption.Load = load
		}
	}
}

func WithCompressLevel(level int) OptionFunc {
	return func(o *Option) {
		o.CompressLevel = level
	}
}

func WithNumPoller(num int) OptionFunc {
	return func(o *Option) {
		o.numPoller = num
	}
}

func WithKeepAlive(keepAlive bool) OptionFunc {
	return func(o *Option) {
		o.KeepAlive = keepAlive
	}
}

func WithWorkMode(mode WorkMode) OptionFunc {
	return func(o *Option) {
		if IsValidWorkMode(mode) {
			o.Mode = mode
		} else {
			o.Mode = ServerMode
		}
	}
}
