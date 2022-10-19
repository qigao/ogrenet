package main

import (
	"github.com/qigao/ogrenet/codecs/modbus"
	"github.com/qigao/ogrenet/utils"
	"github.com/rs/xid"
)

var (
	more        = []byte{0x02, 0x03, 0x00, 0x04}
	header      = []byte{0xAA, 0x01}
	emptyHeader = []byte{0xAA, 0x00}
	cseq        = xid.New()
	emptyCseq   = [12]byte{}
	bytes04     = []byte{0x00, 0x00, 0x00, 0x00}
	body        = []byte{0x01, 0x02, 0x03, 0x04}
)

func MyHeader() []byte {
	bytes := append(header, cseq[:]...) // header + version + cseq
	bytes = append(bytes, more...)      // type + port + body len
	return bytes
}

func MyBody(body []byte) []byte {
	codec := modbus.NewEmptyModbusCodec()
	packet := MyHeader()
	packet = append(packet, body...)
	crc := utils.CheckSum(body)
	tail := modbus.NewTailCodec(crc)
	tailBytes, _ := tail.Encode()
	packet = append(packet, tailBytes...)

	err := codec.Decode(packet)
	if err != nil {
		return nil
	}
	return packet
}
