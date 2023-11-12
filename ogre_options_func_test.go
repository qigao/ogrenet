package ogrenet

import (
	"testing"
	"time"
)

func TestWithCompressLevel(t *testing.T) {
	// Create a new Option struct
	opt := &Option{}

	// Call the WithCompressLevel function with some value
	WithCompressLevel(5)(opt)

	// Check that the value was set correctly
	if opt.CompressLevel != 5 {
		t.Errorf("Expected CompressLevel to be %v, but got %v", 5, opt.CompressLevel)
	}
}

func TestWithNumPoller(t *testing.T) {
	// Create a new Option struct
	opt := &Option{}

	// Call the WithNumPoller function with some value
	WithNumPoller(10)(opt)

	// Check that the value was set correctly
	if opt.numPoller != 10 {
		t.Errorf("Expected numPoller to be %v, but got %v", 10, opt.numPoller)
	}
}

func TestWithKeepAlive(t *testing.T) {
	// Create a new Option struct
	opt := &Option{}

	// Call the WithKeepAlive function with some value
	WithKeepAlive(true)(opt)

	// Check that the value was set correctly
	if opt.KeepAlive != true {
		t.Errorf("Expected KeepAlive to be %v, but got %v", true, opt.KeepAlive)
	}
}

func TestWithWorkMode(t *testing.T) {
	// Create a new Option struct
	opt := &Option{}

	// Call the WithWorkMode function with a valid work mode
	WithWorkMode(WorkMode(1))(opt)

	// Check that the value was set correctly
	if opt.Mode != WorkMode(1) {
		t.Errorf("Expected Mode to be %v, but got %v", WorkMode(1), opt.Mode)
	}

	// Call the WithWorkMode function with an invalid work mode
	WithWorkMode(WorkMode(100))(opt)

	// Check that the value was not set
	if opt.Mode != WorkMode(1) {
		t.Errorf("Expected Mode to be %v, but got %v", WorkMode(1), opt.Mode)
	}
}

func TestWithTimeOut(t *testing.T) {
	// Create a new Option struct
	opt := &Option{}

	// Call the WithTimeOut function with some values
	WithTimeOut(10*time.Second, 5*time.Second)(opt)

	// Check that the values were set correctly
	if opt.TimeOut.Conn != 10*time.Second {
		t.Errorf("Expected Conn timeout to be %v, but got %v", 10*time.Second, opt.TimeOut.Conn)
	}
	if opt.TimeOut.Handle != 5*time.Second {
		t.Errorf("Expected Handle timeout to be %v, but got %v", 5*time.Second, opt.TimeOut.Handle)
	}
}

func TestWithBufSize(t *testing.T) {
	// Create a new Option struct
	opt := &Option{}

	// Call the WithBufSize function with some values
	WithBufSize(1024, 2048, 4096)(opt)

	// Check that the values were set correctly
	if opt.BufSize.PacketSize != 1024 {
		t.Errorf("Expected PacketSize to be %v, but got %v", 1024, opt.BufSize.PacketSize)
	}
	if opt.BufSize.ReadBufSize != 2048 {
		t.Errorf("Expected ReadBufSize to be %v, but got %v", 2048, opt.BufSize.ReadBufSize)
	}
	if opt.BufSize.WriteBufSize != 4096 {
		t.Errorf("Expected WriteBufSize to be %v, but got %v", 4096, opt.BufSize.WriteBufSize)
	}

	// Call the WithBufSize function with some invalid values
	WithBufSize(-1, 0, 1024)(opt)

	// Check that the values were not set
	if opt.BufSize.PacketSize != 1024 {
		t.Errorf("Expected PacketSize to be %v, but got %v", 1024, opt.BufSize.PacketSize)
	}
	if opt.BufSize.ReadBufSize != 2048 {
		t.Errorf("Expected ReadBufSize to be %v, but got %v", 2048, opt.BufSize.ReadBufSize)
	}
	if opt.BufSize.WriteBufSize != 4096 {
		t.Errorf("Expected WriteBufSize to be %v, but got %v", 4096, opt.BufSize.WriteBufSize)
	}
}

func TestWithRotateOptions(t *testing.T) {
	// Create a new Option struct
	opt := &Option{}

	// Call the WithRotateOptions function with some values
	WithLoadBalanceOptions(2, 3, 0.8)(opt)

	// Check that the values were set correctly
	if opt.LBOption.PartitionCount != 2 {
		t.Errorf("Expected PartitionCount to be %v, but got %v", 2, opt.LBOption.PartitionCount)
	}
	if opt.LBOption.ReplicationFactor != 3 {
		t.Errorf("Expected ReplicationFactor to be %v, but got %v", 3, opt.LBOption.ReplicationFactor)
	}
	if opt.LBOption.Load != 0.8 {
		t.Errorf("Expected Load to be %v, but got %v", 0.8, opt.LBOption.Load)
	}

	// Call the WithRotateOptions function with some invalid values
	WithLoadBalanceOptions(0, -1, -0.5)(opt)

	// Check that the values were not set
	if opt.LBOption.PartitionCount != 2 {
		t.Errorf("Expected PartitionCount to be %v, but got %v", 2, opt.LBOption.PartitionCount)
	}
	if opt.LBOption.ReplicationFactor != 3 {
		t.Errorf("Expected ReplicationFactor to be %v, but got %v", 3, opt.LBOption.ReplicationFactor)
	}
	if opt.LBOption.Load != 0.8 {
		t.Errorf("Expected Load to be %v, but got %v", 0.8, opt.LBOption.Load)
	}
}

func TestWithPacket(t *testing.T) {
	// Create a new Option struct
	opt := &Option{}

	// Call the WithPacket function with some values
	WithPacket(PacketType(1), 2, 3)(opt)

	// Check that the values were set correctly
	if opt.Packet.SepType != PacketType(1) {
		t.Errorf("Expected CutType to be %v, but got %v", PacketType(1), opt.Packet.SepType)
	}
	if opt.Packet.Head != 2 {
		t.Errorf("Expected Head to be %v, but got %v", 2, opt.Packet.Head)
	}
	if opt.Packet.Tail != 3 {
		t.Errorf("Expected Tail to be %v, but got %v", 3, opt.Packet.Tail)
	}
}
