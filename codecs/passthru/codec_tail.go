package passthru

import (
	"bytes"
	"encoding/binary"

	"github.com/qigao/ogrenet/shared/crc16"

	"github.com/qigao/ogrenet/errors"
	"github.com/qigao/ogrenet/options"
)

func NewTailCodec(data []byte) *TailCodec {
	crc := crc16.CheckSum(data)
	return &TailCodec{
		CRC:   crc,
		Magic: options.DefaultMagicTail,
	}
}

func NewEmptyTailCodec() *TailCodec {
	return &TailCodec{
		Magic: options.DefaultMagicTail,
	}
}

func (t *TailCodec) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, t)
	return buf.Bytes(), err
}

func (t *TailCodec) Decode(buf []byte) error {
	if (len(buf)) < t.Length() {
		return errors.ErrInvalidCodecTail
	}
	return binary.Read(bytes.NewBuffer(buf), binary.BigEndian, t)
}

func (t *TailCodec) Length() int {
	return (binary.Size(t))
}

func (t *TailCodec) SetCRC(data []byte) {
	t.CRC = crc16.CheckSum(data)
}
