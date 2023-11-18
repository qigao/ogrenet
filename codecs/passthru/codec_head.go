package passthru

import (
	"bytes"
	"encoding/binary"

	"github.com/qigao/ogrenet/codecs"
	"github.com/qigao/ogrenet/shared/errors"
	"github.com/rs/zerolog/log"
)

func NewEmptyHeadCodec() *HeadCodec {
	return &HeadCodec{
		Magic: codecs.DefaultMagicHead,
	}
}

func NewHeadCodec(ver uint8, cmd CmdType, id [4]byte, len uint16, cseq [4]byte) *HeadCodec {
	return &HeadCodec{
		Magic:   codecs.DefaultMagicHead,
		Version: ver,
		CMD:     cmd,
		ID:      id,
		BodyLen: len,
		Cseq:    cseq,
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
	if buf[0] != codecs.DefaultMagicHead {
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
