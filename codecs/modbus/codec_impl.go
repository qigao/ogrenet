package modbus

import (
	"encoding/binary"

	"github.com/dsnet/try"
	"github.com/qigao/ogrenet/errors"
	"github.com/qigao/ogrenet/utils"
	"github.com/rs/zerolog/log"
)

func NewModbusCodec(head *HeadCodec, tail *TailCodec) *CodecModBus {
	return &CodecModBus{
		Head: head,
		Tail: tail,
	}
}

func NewEmptyModbusCodec() *CodecModBus {
	head := NewEmptyHeadCodec()
	tail := NewEmptyTailCodec()
	return NewModbusCodec(head, tail)
}

func (c *CodecModBus) Encode() ([]byte, error) {
	buf := c.GetBody()
	bodyOffset := c.Head.Length()
	bodyLen := uint16(len(buf))
	tailLen := c.Tail.Length()
	msgLen := bodyOffset + bodyLen + tailLen
	c.Head.BodyLen = bodyLen

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

func (c *CodecModBus) Decode(buf []byte) error {
	bodyOffset := c.Head.Length()
	if uint16(len(buf)) < bodyOffset+c.Tail.Length() {
		return errors.ErrIncompletePacket
	}

	err := c.Head.Decode(buf[0:bodyOffset])
	if err != nil {
		return err
	}
	bodyLen := c.Head.BodyLen
	tailLen := c.Tail.Length()
	msgLen := bodyOffset + bodyLen + tailLen

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

func (c *CodecModBus) Length() uint16 {
	return c.Head.Length() + uint16(c.Head.BodyLen) + c.Tail.Length()
}

func (c *CodecModBus) GetBody() []byte {
	return c.Body
}

func (c *CodecModBus) SetBody(body []byte) {
	c.Body = body
}

func (c *CodecModBus) GetHead() *HeadCodec {
	return c.Head
}

func (c *CodecModBus) SetHead(head *HeadCodec) {
	c.Head = head
}

func (c *CodecModBus) GetTail() *TailCodec {
	return c.Tail
}

func (c *CodecModBus) SetTail(tail *TailCodec) {
	c.Tail = tail
}

func (c *CodecModBus) HeaderLength() uint16 {
	return c.Head.Length()
}

func (c *CodecModBus) MsgId() uint64 {
	data := binary.BigEndian.Uint64(c.Head.Cseq[:])
	return data
}
