package modbus

type CodecModBus struct {
	Head *HeadCodec
	Body []byte
	Tail *TailCodec
}

type HeadCodec struct {
	Magic   uint8
	Version uint8
	Cseq    [4]byte
	Type    uint8
	Port    uint8
	BodyLen uint16
}

type TailCodec struct {
	CRC   uint16
	Magic uint8
}
