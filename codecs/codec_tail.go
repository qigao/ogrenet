package codecs

import (
	"bytes"
	"encoding/binary"

	"github.com/qigao/ogrenet/shared/errors"
)

func NewTailCodec(crc uint16) *Tail {
	return &Tail{
		CRC:   crc,
		Magic: MagicTail,
	}
}

func NewEmptyTailCodec() *Tail {
	return &Tail{
		Magic: MagicTail,
	}
}

func (t *Tail) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, t)
	return buf.Bytes(), err
}

func (t *Tail) Decode(buf []byte) error {
	if (len(buf)) < t.Length() {
		return errors.ErrInvalidCodecTail
	}
	return binary.Read(bytes.NewBuffer(buf), binary.BigEndian, t)
}

func (t *Tail) Length() int {
	return (binary.Size(t))
}
