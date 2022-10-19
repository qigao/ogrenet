package modbus

import "github.com/rs/xid"

var (
	more        = []byte{0x02, 0x03, 0x00, 0x04}
	header      = []byte{0xAA, 0x01}
	emptyHeader = []byte{0xAA, 0x00}
	cseq        = xid.New()
	emptyCseq   = [12]byte{}
	bytes04     = []byte{0x00, 0x00, 0x00, 0x00}
	body        = []byte{0x01, 0x02, 0x03, 0x04}
)

func testHeader() []byte {
	expected := append(header, cseq[:]...) // header + version + cseq
	expected = append(expected, more...)   // type + port + body len
	return expected
}

func testEmptyHeader() []byte {
	expected := append(emptyHeader, cseq[:]...)
	expected = append(expected, more...)
	return expected
}

func testBody() []byte {
	packet := append(header, cseq[:]...)
	packet = append(packet, more...)
	packet = append(packet, body...) // body
	return packet
}
