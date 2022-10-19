package codecs

import (
	"bytes"
	"encoding/binary"

	"github.com/qigao/ogrenet/errors"

	"github.com/rs/zerolog/log"
)

type HeadCodec struct {
	Magic   uint8
	Version uint8
	Cseq    [12]byte
	Type    uint8
	Port    uint8
	BodyLen uint16
}

func NewEmptyHeadCodec() *HeadCodec {
	return &HeadCodec{
		Magic: MagicHead,
	}
}

func NewHeadCodec(ver, cmd, port uint8, len uint16, cseq [12]byte) *HeadCodec {
	return &HeadCodec{
		Magic:   MagicHead,
		Version: ver,
		Cseq:    cseq,
		Type:    cmd,
		Port:    port,
		BodyLen: len,
	}
}

func (h *HeadCodec) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, h)
	return buf.Bytes(), err
}

func (h *HeadCodec) Decode(buf []byte) error {
	if len(buf) < h.Length() {
		log.Error().Msgf("invalid head length %d", len(buf))
		return errors.ErrIncompletePacket
	}
	if buf[0] != MagicHead {
		log.Error().Msgf("invalid head magic number %v", buf[0])
		return errors.ErrInvalidMagicNumber
	}
	err := binary.Read(bytes.NewBuffer(buf), binary.BigEndian, h)
	if err != nil {
		log.Error().Err(err).Msgf("failed to decode head codecs %v", buf)
		return errors.ErrInvalidCodecHead
	}
	return nil
}

func (h *HeadCodec) Length() int {
	return binary.Size(h)
}
