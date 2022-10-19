package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCRC16(t *testing.T) {
	t.Run("test simple words", func(t *testing.T) {
		data := []byte("Modbus CRC16 generation")
		crc16 := CheckSum(data)
		assert.Equal(t, uint16(0x6d80), crc16)
	})
}
