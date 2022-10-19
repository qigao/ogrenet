package codecs

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestHeadCodecEncode(t *testing.T) {
	t.Run("Test encode", func(t *testing.T) {
		h := NewHeadCodec(1, 2, 3, 4, cseq)
		t.Logf("head len: %d", h.Length())
		expected := append(header, cseq.Bytes()...)
		expected = append(expected, more...)
		byteResult, err := h.Encode()
		assert.NoError(t, err)
		equal := reflect.DeepEqual(byteResult, expected)
		assert.True(t, equal)
	})
	t.Run("Test Binary Size", func(t *testing.T) {
		h := NewHeadCodec(1, 2, 3, 4, cseq)
		expected := 18
		result := h.Length()
		assert.Equal(t, expected, result)
	})
}

func TestHeadCodecDecode(t *testing.T) {
	t.Run("Test decode", func(t *testing.T) {
		h := &HeadCodec{}
		buf := append(header, cseq.Bytes()...)
		buf = append(buf, more...)
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
		t.Logf("head len: %d", h.Length())
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
