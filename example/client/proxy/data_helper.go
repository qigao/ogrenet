package main

import (
	"encoding/binary"
	"github.com/qigao/ogrenet/codecs"
	"time"

	"github.com/qigao/ogrenet/shared/crc16"
	"github.com/rs/zerolog/log"
)

var (
	more    = []byte{0x02, 0x03, 0x00, 0x04}
	header  = []byte{0xAA, 0x01}
	cseq    = [4]byte{}
	body    = []byte{0x01, 0x02, 0x03, 0x04}
	current = time.Now().Unix()
)

func MyHeader() []byte {
	binary.BigEndian.PutUint32(cseq[:], uint32(current))
	bytes := append(header, cseq[:]...) // header + version + cseq
	bytes = append(bytes, more...)      // type + port + body len
	return bytes
}

func DemoMsg(body []byte) []byte {
	codec := codecs.NewEmptyModbusCodec()
	packet := MyHeader()
	packet = append(packet, body...)
	crc := crc16.CheckSum(body)
	tail := codecs.NewTailCodec(crc)
	tailBytes, _ := tail.Encode()
	packet = append(packet, tailBytes...)

	err := codec.Decode(packet)
	if err != nil {
		log.Error().Err(err)
		return nil
	}
	return packet
}
