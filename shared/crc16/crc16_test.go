package crc16

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
	t.Run("empty data", func(t *testing.T) {
		data := []byte{}
		crc16 := CheckSum(data)
		assert.Equal(t, uint16(0xffff), crc16)
	})
	t.Run("test nil data", func(t *testing.T) {
		crc16 := CheckSum(nil)
		assert.Equal(t, uint16(0xffff), crc16)
	})
	t.Run("test 0x00", func(t *testing.T) {
		data := []byte{0x00}
		crc16 := CheckSum(data)
		assert.Equal(t, uint16(0x40bf), crc16)
	})
}

func BenchmarkCheckSum(b *testing.B) {
	data := []byte("Modbus CRC16 generation")
	for n := 0; n < b.N; n++ {
		for i := 0; i < 1000; i++ {
			CheckSum(data)
		}
	}
}
