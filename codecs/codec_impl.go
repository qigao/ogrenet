package codecs

import (
	"encoding/binary"

	"github.com/dsnet/try"
	"github.com/qigao/ogrenet/shared/crc16"
	"github.com/qigao/ogrenet/shared/errors"
	"github.com/rs/zerolog/log"
)

func NewModbusCodec(head *Head, tail *Tail) *ModBus {
	return &ModBus{
		Head: head,
		Tail: tail,
	}
}

func NewEmptyModbusCodec() *ModBus {
	head := NewEmptyHeadCodec()
	tail := NewEmptyTailCodec()
	return NewModbusCodec(head, tail)
}

func (c *ModBus) Encode() ([]byte, error) {
	buf := c.GetBody()
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
	c.Tail.CRC = crc16.CheckSum(data[bodyOffset : msgLen-tailLen])
	// write tail
	magicTail := try.E1(c.Tail.Encode())
	copy(data[msgLen-tailLen:], magicTail)
	return data, nil
}

func (c *ModBus) Decode(buf []byte) error {
	bodyOffset := c.Head.Length()
	if (len(buf)) < bodyOffset+c.Tail.Length() {
		return errors.ErrIncompletePacket
	}

	err := c.Head.Decode(buf[0:bodyOffset])
	if err != nil {
		return err
	}
	bodyLen := int(c.Head.BodyLen)
	tailLen := c.Tail.Length()
	msgLen := bodyOffset + bodyLen + tailLen
	if bodyLen > len(buf) {
		return errors.ErrIncompletePacket
	}
	body := buf[bodyOffset : msgLen-tailLen]
	crc := crc16.CheckSum(body)
	tail := buf[msgLen-tailLen:]
	try.E(c.Tail.Decode(tail))
	if crc != c.Tail.CRC {
		log.Error().Msgf("invalid packet tail: %v", errors.ErrInvalidCRCValue)
		return errors.ErrInvalidCRCValue
	}
	c.Body = body
	return nil
}

func (c *ModBus) Length() int {
	return c.Head.Length() + int(c.Head.BodyLen) + c.Tail.Length()
}

func (c *ModBus) GetBody() []byte {
	return c.Body
}

func (c *ModBus) SetBody(body []byte) {
	c.Body = body
}

func (c *ModBus) GetHead() *Head {
	return c.Head
}

func (c *ModBus) SetHead(head *Head) {
	c.Head = head
}

func (c *ModBus) GetTail() *Tail {
	return c.Tail
}

func (c *ModBus) SetTail(tail *Tail) {
	c.Tail = tail
}

func (c *ModBus) HeaderLength() int {
	return c.Head.Length()
}

func (c *ModBus) MsgId() uint32 {
	return binary.BigEndian.Uint32(c.Head.Cseq[:])
}
