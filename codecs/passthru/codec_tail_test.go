package passthru

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTailPart(t *testing.T) {
	t.Run("decode tail", func(t *testing.T) {
		data := []byte{0x12, 0x34, 0x55}
		tailPart := &TailCodec{}
		err := tailPart.Decode(data)
		assert.NoError(t, err)
		assert.Equal(t, uint16(0x1234), tailPart.CRC)
		assert.Equal(t, uint8(0x55), tailPart.Magic)
	})
	t.Run("tail length", func(t *testing.T) {
		tailPart := &TailCodec{}
		assert.Equal(t, 3, tailPart.Length())
	})
}
