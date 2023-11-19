package ogrenet

import "testing"

func TestNewNetConnWithTerm(t *testing.T) {
	msgPool := NewMessagePool()
	msgChan := make(chan *MsgConn)
	t.Run("default limiter", func(t *testing.T) {
		limiter := SetupLimiterOptions(&Option{
			TimeOut: TimeOut{
				Conn:   1,
				Handle: 2,
			},
			Packet: Packet{
				SepType: PacketType(3),
				Head:    4,
				Tail:    5,
			},
			BufSize: BufSize{
				PacketSize:   6,
				ReadBufSize:  7,
				WriteBufSize: 8,
			},
		})
		conn := NewNetConnWithTerm(1, msgPool, msgChan, &limiter)

		if conn == nil {
			t.Fatal("NewNetConnWithTerm returned nil")
		}
		if conn.limiter.Packet.SepType != limiter.Packet.SepType {
			t.Errorf("NewNetConnWithTerm returned cut type %d, expected %d", conn.limiter.Packet.SepType, limiter.Packet.SepType)
		}
		if conn.limiter.BufSize.PacketSize != 6 {
			t.Errorf("NewNetConnWithTerm returned packet size %d, expected %d", conn.limiter.BufSize.PacketSize, DefaultPacketSize)
		}
	})
	t.Run("empty limiter", func(t *testing.T) {
		limiter := Limiter{}
		conn := NewNetConnWithTerm(1, msgPool, msgChan, &limiter)

		if conn == nil {
			t.Fatal("NewNetConnWithTerm returned nil")
		}
		if conn.limiter.Packet.SepType != limiter.Packet.SepType {
			t.Errorf("NewNetConnWithTerm returned cut type %d, expected %d", conn.limiter.Packet.SepType, limiter.Packet.SepType)
		}
		if conn.limiter.BufSize.PacketSize != 1024 {
			t.Errorf("NewNetConnWithTerm returned packet size %d, expected %d", conn.limiter.BufSize.PacketSize, DefaultPacketSize)
		}
	})
}

func TestShouldCutByTail(t *testing.T) {
	conn := &Conn{
		limiter: Limiter{
			Packet: Packet{
				SepType: SepByTail,
				Head:    0,
				Tail:    1,
			},
		},
	}
	if !conn.shouldSepByTail() {
		t.Errorf("ShouldCutByTail returned false, expected true")
	}
}

func TestShouldCutByHeadAndTail(t *testing.T) {
	conn := &Conn{
		limiter: Limiter{
			Packet: Packet{
				SepType: SepByHeadAndTail,
				Head:    1,
				Tail:    1,
			},
		},
	}
	if !conn.shouldSepByHeadAndTail() {
		t.Errorf("shouldCutByHeadAndTail returned false, expected true")
	}

	conn = &Conn{
		limiter: Limiter{
			Packet: Packet{
				SepType: SepByHeadAndTail,
				Head:    0,
				Tail:    1,
			},
		},
	}
	if conn.shouldSepByHeadAndTail() {
		t.Errorf("shouldCutByHeadAndTail returned true, expected false")
	}

	conn = &Conn{
		limiter: Limiter{
			Packet: Packet{
				SepType: SepByHeadAndTail,
				Head:    1,
				Tail:    0,
			},
		},
	}
	if conn.shouldSepByHeadAndTail() {
		t.Errorf("shouldCutByHeadAndTail returned true, expected false")
	}

	conn = &Conn{
		limiter: Limiter{
			Packet: Packet{
				SepType: SepByTail,
				Head:    1,
				Tail:    1,
			},
		},
	}
	if conn.shouldSepByHeadAndTail() {
		t.Errorf("shouldCutByHeadAndTail returned true, expected false")
	}
}
