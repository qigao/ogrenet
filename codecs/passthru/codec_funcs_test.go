package passthru

import (
	"testing"

	"github.com/qigao/ogrenet/codecs"
	"github.com/stretchr/testify/assert"
)

var (
	id   = [4]byte{0x12, 0x34, 0x56, 0x78}
	HEAD = 0xAA
	TAIL = 0x55
)

func TestNewPassThruCodec(t *testing.T) {
	codec := NewPassThruHead(UnknownType, id, 0)
	assert.Equal(t, codec.CMD, UnknownType)
	assert.Equal(t, codec.ID, id)
	assert.Equal(t, codec.BodyLen, uint16(0))
	assert.Equal(t, codec.Magic, uint8(HEAD))
}

func TestNewRegisterCodec(t *testing.T) {
	codec := NewRegisterCodec(id)
	assert.Equal(t, codec.Head.CMD, Register)
	assert.Equal(t, codec.Head.ID, id)
	assert.Equal(t, codec.Head.BodyLen, uint16(1))
	assert.Equal(t, uint8(HEAD), codec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, codec.Body)
	assert.Equal(t, uint16(0x40bf), codec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), codec.Tail.Magic)
	revCodec := NewEmptyPassThruCodec()
	data, _ := codec.Encode()
	revCodec.Decode(data)
	assert.Equal(t, Register, revCodec.Head.CMD)
	assert.Equal(t, id, revCodec.Head.ID)
	assert.Equal(t, uint16(1), revCodec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), revCodec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, revCodec.Body)
	assert.Equal(t, uint16(0x40bf), revCodec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), revCodec.Tail.Magic)
}

func TestNewAckCodec(t *testing.T) {
	t.Run("simple ack test", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03}
		codec := NewAckCodec(id, data)
		assert.Equal(t, Ack, codec.Head.CMD)
		assert.Equal(t, id, codec.Head.ID)
		assert.Equal(t, uint16(3), codec.Head.BodyLen)
		assert.Equal(t, uint8(HEAD), codec.Head.Magic)
		assert.Equal(t, codecs.ZeroBytes, codec.Body)
		assert.Equal(t, uint8(TAIL), codec.Tail.Magic)
	})
	t.Run("simple ack tst when data is codectype", func(t *testing.T) {
		data := []byte{byte(Register)}
		codec := NewAckCodec(id, data)
		assert.Equal(t, Ack, codec.Head.CMD)
		assert.Equal(t, id, codec.Head.ID)
		assert.Equal(t, uint16(1), codec.Head.BodyLen)
		assert.Equal(t, uint8(HEAD), codec.Head.Magic)
		assert.Equal(t, codecs.ZeroBytes, codec.Body)
		assert.Equal(t, uint8(TAIL), codec.Tail.Magic)
	})
}

func TestNewUnRegisterCodec(t *testing.T) {
	codec := NewUnRegisterCodec(id)
	assert.Equal(t, UnRegister, codec.Head.CMD)
	assert.Equal(t, id, codec.Head.ID)
	assert.Equal(t, uint16(1), codec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), codec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, codec.Body)
	assert.Equal(t, uint16(0x40bf), codec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), codec.Tail.Magic)
	revCodec := NewEmptyPassThruCodec()
	data, _ := codec.Encode()
	revCodec.Decode(data)
	assert.Equal(t, UnRegister, revCodec.Head.CMD)
	assert.Equal(t, id, revCodec.Head.ID)
	assert.Equal(t, uint16(1), revCodec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), revCodec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, revCodec.Body)
	assert.Equal(t, uint16(0x40bf), revCodec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), revCodec.Tail.Magic)
}

func TestNewHeartBeatCodec(t *testing.T) {
	codec := NewHeartBeatCodec(id)
	assert.Equal(t, HeartBeat, codec.Head.CMD)
	assert.Equal(t, id, codec.Head.ID)
	assert.Equal(t, uint16(1), codec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), codec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, codec.Body)
	assert.Equal(t, uint16(0x40bf), codec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), codec.Tail.Magic)
	revCodec := NewEmptyPassThruCodec()
	data, _ := codec.Encode()
	revCodec.Decode(data)
	assert.Equal(t, HeartBeat, revCodec.Head.CMD)
	assert.Equal(t, id, revCodec.Head.ID)
	assert.Equal(t, uint16(1), revCodec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), revCodec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, revCodec.Body)
	assert.Equal(t, uint16(0x40bf), revCodec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), revCodec.Tail.Magic)
}

func TestNewDataCodec(t *testing.T) {
	body := []byte{0x01, 0x02, 0x03}
	codec := NewDataCodec(id, body)
	data, _ := codec.Encode()
	assert.Equal(t, Data, codec.Head.CMD)
	assert.Equal(t, id, codec.Head.ID)
	assert.Equal(t, uint16(3), codec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), codec.Head.Magic)
	assert.Equal(t, body, codec.Body)
	assert.Equal(t, uint8(TAIL), codec.Tail.Magic)

	revCodec := NewEmptyPassThruCodec()
	revCodec.Decode(data)
	assert.Equal(t, Data, revCodec.Head.CMD)
	assert.Equal(t, id, revCodec.Head.ID)
	assert.Equal(t, uint16(3), revCodec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), revCodec.Head.Magic)
	assert.Equal(t, body, revCodec.Body)
	assert.Equal(t, uint8(TAIL), revCodec.Tail.Magic)
}

func TestNewCloseCodec(t *testing.T) {
	codec := NewCloseCodec(id)
	assert.Equal(t, Close, codec.Head.CMD)
	assert.Equal(t, id, codec.Head.ID)
	assert.Equal(t, uint16(1), codec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), codec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, codec.Body)
	assert.Equal(t, uint16(0x40bf), codec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), codec.Tail.Magic)
	revCodec := NewEmptyPassThruCodec()
	data, _ := codec.Encode()
	revCodec.Decode(data)
	assert.Equal(t, Close, revCodec.Head.CMD)
	assert.Equal(t, id, revCodec.Head.ID)
	assert.Equal(t, uint16(1), revCodec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), revCodec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, revCodec.Body)
	assert.Equal(t, uint16(0x40bf), revCodec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), revCodec.Tail.Magic)
}

func TestNewReConnectCodec(t *testing.T) {
	codec := NewReConnectCodec(id)
	assert.Equal(t, ReConnect, codec.Head.CMD)
	assert.Equal(t, id, codec.Head.ID)
	assert.Equal(t, uint16(1), codec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), codec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, codec.Body)
	assert.Equal(t, uint16(0x40bf), codec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), codec.Tail.Magic)
	revCodec := NewEmptyPassThruCodec()
	data, _ := codec.Encode()
	revCodec.Decode(data)
	assert.Equal(t, ReConnect, revCodec.Head.CMD)
	assert.Equal(t, id, revCodec.Head.ID)
	assert.Equal(t, uint16(1), revCodec.Head.BodyLen)
	assert.Equal(t, uint8(HEAD), revCodec.Head.Magic)
	assert.Equal(t, codecs.ZeroBytes, revCodec.Body)
	assert.Equal(t, uint16(0x40bf), revCodec.Tail.CRC)
	assert.Equal(t, uint8(TAIL), revCodec.Tail.Magic)
}
