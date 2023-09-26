package codecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTailPart(t *testing.T) {
	t.Run("decode tail", func(t *testing.T) {
		crc := uint16(0x1234)
		tailPart := NewTailCodec(crc)
		buf, err := tailPart.Encode()
		assert.NoError(t, err)
		expected := []byte{0x12, 0x34, 0x55}
		assert.Equal(t, expected, buf)
	})
	t.Run("encode tail", func(t *testing.T) {
		buf := []byte{0x12, 0x34, 0x55}
		tailPart := &TailCodec{}
		err := tailPart.Decode(buf)
		assert.NoError(t, err)
		expected := NewTailCodec(0x1234)
		assert.Equal(t, expected, tailPart)
	})
	t.Run("tail length", func(t *testing.T) {
		tailPart := &TailCodec{}
		expected := 3
		assert.Equal(t, expected, tailPart.Length())
	})
}
