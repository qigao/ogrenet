package ogrenet

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProxyOptions(t *testing.T) {
	t.Run("test default values", func(t *testing.T) {
		po := ProxyOptions{}
		assert.Equal(t, ProxyNone, po.mode)
		assert.Equal(t, "", po.pattern)
	})
	t.Run("test setting values", func(t *testing.T) {
		po := ProxyOptions{
			mode:    Publish,
			pattern: "example.com",
		}
		assert.Equal(t, Publish, po.mode)
		assert.Equal(t, "example.com", po.pattern)
	})
}

func TestOptions(t *testing.T) {
	t.Run("test default values", func(t *testing.T) {
		o := Options{}
		assert.Equal(t, TimeOut{}, o.TimeOut)
		assert.Equal(t, Packet{}, o.Packet)
		assert.Equal(t, BufSize{}, o.BufSize)
		assert.False(t, o.KeepAlive)
		assert.Equal(t, ProxyOptions{}, o.proxy)
		assert.Equal(t, ProxyNone, o.proxy.mode)
		assert.Equal(t, 0, o.CompressLevel)
		assert.Equal(t, 0, o.numPoller)
		assert.Equal(t, RotateConfig{}, o.rotateCfg)
		assert.Equal(t, 0, o.rotateCfg.PartitionCount)
	})
}

func TestSetupLimiterOptions(t *testing.T) {
	t.Run("test nil options", func(t *testing.T) {
		limiter := SetupLimiterOptions(nil)
		assert.Equal(t, Limiter{}, limiter)
	})
	t.Run("test empty options", func(t *testing.T) {
		limiter := SetupLimiterOptions(&Options{})
		assert.Equal(t, Limiter{}, limiter)
	})
	t.Run("test setting values", func(t *testing.T) {
		limiter := SetupLimiterOptions(&Options{
			TimeOut: TimeOut{
				Conn:   1,
				Handle: 2,
			},
			Packet: Packet{
				CutType: PacketType(3),
				Head:    4,
				Tail:    5,
			},
			BufSize: BufSize{
				PacketSize:   6,
				ReadBufSize:  7,
				WriteBufSize: 8,
			},
		})
		assert.Equal(t, TimeOut{
			Conn:   1,
			Handle: 2,
		}, limiter.Timeout)
		assert.Equal(t, Packet{
			CutType: PacketType(3),
			Head:    4,
			Tail:    5,
		}, limiter.Packet)
		assert.Equal(t, BufSize{
			PacketSize:   6,
			ReadBufSize:  7,
			WriteBufSize: 8,
		}, limiter.BufSize)
	})
}
