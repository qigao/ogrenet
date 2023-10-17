package passthru

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var id = [4]byte{0x12, 0x34, 0x56, 0x78}

func TestNewPassThruCodec(t *testing.T) {
	codec := NewPassThruHead(Register, id, 0)
	assert.Equal(t, codec.CodecType, Register)
	assert.Equal(t, codec.ID, id)
	assert.Equal(t, codec.BodyLen, uint16(0))
	assert.Equal(t, codec.Magic, uint8(0xAA))
}

func TestNewRegisterCodec(t *testing.T) {
	codec := NewRegisterCodec(id)
	assert.Equal(t, codec.Head.CodecType, Register)
	assert.Equal(t, codec.Head.ID, id)
	assert.Equal(t, codec.Head.BodyLen, uint16(0))
	assert.Equal(t, codec.Head.Magic, uint8(0xAA))
	assert.Nil(t, codec.Body)
	assert.Equal(t, codec.Tail.CRC, uint16(0))
	assert.Equal(t, codec.Tail.Magic, uint8(0x55))
}

func TestNewAckCodec(t *testing.T) {
	codec := NewAckCodec(id)
	assert.Equal(t, codec.Head.CodecType, Ack)
	assert.Equal(t, codec.Head.ID, id)
	assert.Equal(t, codec.Head.BodyLen, uint16(0))
	assert.Equal(t, codec.Head.Magic, uint8(0xAA))
	assert.Nil(t, codec.Body)
	assert.Equal(t, codec.Tail.CRC, uint16(0))
	assert.Equal(t, codec.Tail.Magic, uint8(0x55))
}

func TestNewUnRegisterCodec(t *testing.T) {
	codec := NewUnRegisterCodec(id)
	assert.Equal(t, codec.Head.CodecType, Unregister)
	assert.Equal(t, codec.Head.ID, id)
	assert.Equal(t, codec.Head.BodyLen, uint16(0))
	assert.Equal(t, codec.Head.Magic, uint8(0xAA))
	assert.Nil(t, codec.Body)
	assert.Equal(t, codec.Tail.CRC, uint16(0))
	assert.Equal(t, codec.Tail.Magic, uint8(0x55))
}

func TestNewHeartBeatCodec(t *testing.T) {
	codec := NewHeartBeatCodec(id)
	assert.Equal(t, codec.Head.CodecType, HeartBeat)
	assert.Equal(t, codec.Head.ID, id)
	assert.Equal(t, codec.Head.BodyLen, uint16(0))
	assert.Equal(t, codec.Head.Magic, uint8(0xAA))
	assert.Nil(t, codec.Body)
	assert.Equal(t, codec.Tail.CRC, uint16(0))
	assert.Equal(t, codec.Tail.Magic, uint8(0x55))
}

func TestNewDataCodec(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03}
	codec := NewDataCodec(id, data)
	assert.Equal(t, codec.Head.CodecType, Data)
	assert.Equal(t, codec.Head.ID, id)
	assert.Equal(t, codec.Head.BodyLen, uint16(3))
	assert.Equal(t, codec.Head.Magic, uint8(0xAA))
	assert.Equal(t, codec.Body, data)
	assert.Equal(t, codec.Tail.Magic, uint8(0x55))
}

func TestNewCloseCodec(t *testing.T) {
	codec := NewCloseCodec(id)
	assert.Equal(t, codec.Head.CodecType, Close)
	assert.Equal(t, codec.Head.ID, id)
	assert.Equal(t, codec.Head.BodyLen, uint16(0))
	assert.Equal(t, codec.Head.Magic, uint8(0xAA))
	assert.Nil(t, codec.Body)
	assert.Equal(t, codec.Tail.CRC, uint16(0))
	assert.Equal(t, codec.Tail.Magic, uint8(0x55))
}

func TestNewReConnectCodec(t *testing.T) {
	codec := NewReConnectCodec(id)
	assert.Equal(t, codec.Head.CodecType, ReConnect)
	assert.Equal(t, codec.Head.ID, id)
	assert.Equal(t, codec.Head.BodyLen, uint16(0))
	assert.Equal(t, codec.Head.Magic, uint8(0xAA))
	assert.Nil(t, codec.Body)
	assert.Equal(t, codec.Tail.CRC, uint16(0))
	assert.Equal(t, codec.Tail.Magic, uint8(0x55))
}
