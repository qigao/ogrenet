package codecs

import (
	"bytes"
	"encoding/binary"

	"github.com/qigao/ogrenet/shared/errors"
	"github.com/rs/zerolog/log"
)

func NewEmptyHeadCodec() *Head {
	return &Head{
		Magic: MagicHead,
	}
}

func NewHeadCodec(ver, cmd, port uint8, len uint16, cseq [4]byte) *Head {
	return &Head{
		Magic:   MagicHead,
		Version: ver,
		Type:    cmd,
		Port:    port,
		BodyLen: len,
		Cseq:    cseq,
	}
}

func (h *Head) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, h)
	return buf.Bytes(), err
}

func (h *Head) Decode(buf []byte) error {
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

func (h *Head) Length() int {
	return binary.Size(h)
}
