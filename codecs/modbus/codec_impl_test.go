package modbus

import (
	"bytes"
	"testing"

	"github.com/qigao/ogrenet/errors"
	"github.com/qigao/ogrenet/utils"

	"github.com/stretchr/testify/assert"
)

func TestInvalidSimpleCodec(t *testing.T) {
	codec := NewEmptyModbusCodec()
	t.Run("Incomplete packet", func(t *testing.T) {
		packet := []byte{
			0xAA, 0x34, 0x56, 0x79, // Incomplete MagicHeadBytes
			0x00, 0x00, 0x00, 0x04, // BodyLen
			0x01, 0x02, 0x03, 0x04, // Body
			0x78, 0x56, 0x34, 0x55, // MagicTailBytes
		}
		err := codec.Decode(packet)
		assert.Error(t, errors.ErrIncompletePacket, err)
	})

	t.Run("Invalid Magic number", func(t *testing.T) {
		packet := []byte{
			0x12, 0x34, // Invalid header
			0x00, 0x00, 0x00, 0x04, // cseq
			0x00, 0x00, 0x00, 0x04, // BodyLen
			0x01, 0x02, 0x03, 0x04, // Body
			0x78, 0x56, 0x34, 0x55, // MagicTailBytes
		}
		err := codec.Decode(packet)
		assert.Error(t, errors.ErrInvalidCodecHead, err)
	})
}

func TestSimpleCodec(t *testing.T) {
	t.Run("Encode packet", func(t *testing.T) {
		codec := NewEmptyModbusCodec()
		codec.SetBody(more)
		packet, err := codec.Encode()
		assert.NoError(t, err)
		result := append(emptyHeader, emptyCseq[:]...)
		result = append(result, 0x00, 0x00, 0x00, 0x04)
		result = append(result, more...)
		crc := utils.CheckSum(more)
		tail := NewTailCodec(crc)
		tailBytes, _ := tail.Encode()
		result = append(result, tailBytes...)
		assert.Equal(t, bytes.Equal(packet, result), true)
	})
	t.Run("Decode packet", func(t *testing.T) {
		codec := NewEmptyModbusCodec()
		packet := testBody()
		crc := utils.CheckSum(body)
		tail := NewTailCodec(crc)
		tailBytes, _ := tail.Encode()
		packet = append(packet, tailBytes...)
		t.Logf("packet: %+v", packet)
		err := codec.Decode(packet)
		assert.Nil(t, err)
		assert.Equal(t, bytes.Equal(codec.Body, body), true)
	})
}
