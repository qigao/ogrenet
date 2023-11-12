package modbus

import (
	"encoding/binary"
	"time"
)

var (
	more        = []byte{0x02, 0x03, 0x00, 0x04}
	header      = []byte{0xAA, 0x01}
	emptyHeader = []byte{0xAA, 0x00}
	emptyCseq   = [4]byte{}
	bytes04     = []byte{0x00, 0x00, 0x00, 0x00}
	body        = []byte{0x01, 0x02, 0x03, 0x04}
	cseq        = [4]byte{}
	current     = time.Now().Unix()
)

func testHeader() []byte {
	binary.BigEndian.PutUint32(cseq[:], uint32(current))
	expected := append(header, cseq[:]...) // header + version + cseq
	expected = append(expected, more...)   // type + port + body len
	return expected
}

func testEmptyHeader() []byte {
	binary.BigEndian.PutUint32(cseq[:], uint32(current))
	expected := append(emptyHeader, cseq[:]...)
	expected = append(expected, more...)
	return expected
}

func testBody() []byte {
	binary.BigEndian.PutUint32(cseq[:], uint32(current))
	packet := append(header, cseq[:]...)
	packet = append(packet, more...)
	packet = append(packet, body...) // body
	return packet
}
