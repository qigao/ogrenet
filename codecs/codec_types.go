package codecs

import (
	"encoding/binary"
	"time"
)

type (
	CSEQ = [4]byte
	ID   = [4]byte
)

var (
	Empty     = [4]byte{0x00, 0x00, 0x00, 0x00}
	ZeroBytes = []byte{0x00}
)

const ZeroCRC16 = 0x40bf
const (
	MagicHead = 0xAA
	MagicTail = 0x55
)

var (
	Ping = []byte{0x10, 0x10, 0x10, 0x10}
	Pong = []byte{0x01, 0x01, 0x01, 0x01}
)

func TimeBasedCseq() [4]byte {
	cseq := make([]byte, 4)
	current := time.Now().Unix()
	binary.BigEndian.PutUint32(cseq, uint32(current))
	return [4]byte{cseq[0], cseq[1], cseq[2], cseq[3]}
}
