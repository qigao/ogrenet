package codecs

import (
	"github.com/dsnet/try"
	"github.com/qigao/ogrenet/errors"
	"github.com/qigao/ogrenet/utils"
	"github.com/rs/zerolog/log"
)

func NewModbusCodec(head *HeadCodec, tail *TailCodec) *ModbusCodec {
	return &ModbusCodec{
		Head: head,
		Tail: tail,
	}
}

func NewEmptyModbusCodec() *ModbusCodec {
	head := NewEmptyHeadCodec()
	tail := NewEmptyTailCodec()
	return NewModbusCodec(head, tail)
}

func (c *ModbusCodec) Encode(buf []byte) ([]byte, error) {
	bodyOffset := c.Head.Length()
	bodyLen := len(buf)
	tailLen := c.Tail.Length()
	msgLen := bodyOffset + bodyLen + tailLen
	c.Head.BodyLen = uint16(bodyLen)

	// write head	bytes
	data := make([]byte, msgLen)
	magicHead := try.E1(c.Head.Encode())
	copy(data, magicHead)

	// write body bytes
	copy(data[bodyOffset:msgLen], buf)
	// write crc16
	c.Tail.CRC = utils.CheckSum(data[bodyOffset : msgLen-tailLen])
	// write tail
	magicTail := try.E1(c.Tail.Encode())
	copy(data[msgLen-tailLen:], magicTail)
	return data, nil
}

func (c *ModbusCodec) Decode(buf []byte) error {
	bodyOffset := c.Head.Length()
	if len(buf) < bodyOffset+c.Tail.Length() {
		return errors.ErrIncompletePacket
	}

	err := c.Head.Decode(buf[0:bodyOffset])
	if err != nil {
		return err
	}
	bodyLen := c.Head.BodyLen
	tailLen := c.Tail.Length()
	msgLen := bodyOffset + int(bodyLen) + tailLen

	body := buf[bodyOffset : msgLen-tailLen]
	crc := utils.CheckSum(body)
	tail := buf[msgLen-tailLen:]
	try.E(c.Tail.Decode(tail))
	if crc != c.Tail.CRC {
		log.Error().Msgf("invalid packet tail: %v", errors.ErrInvalidCRCValue)
		return errors.ErrInvalidCRCValue
	}
	c.Body = append(c.Body, body...)
	return nil
}
