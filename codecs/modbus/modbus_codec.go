package modbus

type CodecModBus struct {
	Head *HeadCodec
	Body []byte
	Tail *TailCodec
}

const (
	MagicHead = 0xAA
	MagicTail = 0x55
)

type HeadCodec struct {
	Magic   uint8
	Version uint8
	Cseq    [12]byte
	Type    uint8
	Port    uint8
	BodyLen uint16
}

type TailCodec struct {
	CRC   uint16
	Magic uint8
}
