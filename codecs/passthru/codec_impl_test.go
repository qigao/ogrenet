package passthru

import (
	"testing"

	"github.com/qigao/ogrenet/errors"
	"github.com/stretchr/testify/assert"
)

func TestInvalidSimpleCodec(t *testing.T) {
	codec := NewEmptyPassThruCodec()
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
