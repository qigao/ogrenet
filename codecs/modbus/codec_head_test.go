package modbus

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestHeadCodecEncode(t *testing.T) {
	binary.BigEndian.PutUint32(cseq[:], uint32(current))
	t.Run("Test encode", func(t *testing.T) {
		h := NewHeadCodec(1, 2, 3, 4, cseq)
		expected := testHeader()
		byteResult, err := h.Encode()
		assert.NoError(t, err)
		equal := reflect.DeepEqual(byteResult, expected)
		assert.True(t, equal)
	})
	t.Run("Test Binary Size", func(t *testing.T) {
		h := NewHeadCodec(1, 2, 3, 4, cseq)
		assert.Equal(t, 10, h.Length())
	})
}

func TestHeadCodecDecode(t *testing.T) {
	binary.BigEndian.PutUint32(cseq[:], uint32(current))
	t.Run("Test decode", func(t *testing.T) {
		h := &HeadCodec{}
		buf := testHeader()
		if err := h.Decode(buf); err != nil {
			t.Errorf("Decode() error = %v", err)
		}
		expected := NewHeadCodec(1, 2, 3, 4, cseq)
		if !cmp.Equal(h, expected) {
			t.Errorf("Decode() = %v, want %v", h, expected)
		}
	})
}

func TestEmptyHead(t *testing.T) {
	t.Run("Test empty new head part", func(t *testing.T) {
		h := NewEmptyHeadCodec()
		byteResult, err := h.Encode()
		assert.NoError(t, err)
		assert.Equal(t, h.Length(), len(byteResult))
	})
	t.Run("Test empty decode", func(t *testing.T) {
		h := &HeadCodec{}
		buf := []byte{0xAA, 0x00}
		buf = append(buf, emptyCseq[:]...)
		buf = append(buf, bytes04...)
		if err := h.Decode(buf); err != nil {
			t.Errorf("Decode() error = %v", err)
		}
		expected := NewEmptyHeadCodec()
		if !reflect.DeepEqual(h, expected) {
			t.Errorf("Decode() = %v, want %v", h, expected)
		}
	})
}
