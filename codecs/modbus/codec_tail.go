package modbus

import (
	"bytes"
	"encoding/binary"

	"github.com/qigao/ogrenet/codecs"

	"github.com/qigao/ogrenet/errors"
)

func NewTailCodec(crc uint16) *TailCodec {
	return &TailCodec{
		CRC:   crc,
		Magic: codecs.DefaultMagicTail,
	}
}

func NewEmptyTailCodec() *TailCodec {
	return &TailCodec{
		Magic: codecs.DefaultMagicTail,
	}
}

func (t *TailCodec) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, t)
	return buf.Bytes(), err
}

func (t *TailCodec) Decode(buf []byte) error {
	if uint16(len(buf)) < t.Length() {
		return errors.ErrInvalidCodecTail
	}
	return binary.Read(bytes.NewBuffer(buf), binary.BigEndian, t)
}

func (t *TailCodec) Length() uint16 {
	return uint16(binary.Size(t))
}
