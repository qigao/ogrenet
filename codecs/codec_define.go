package codecs

const (
	MagicHead = 0xAA
	MagicTail = 0x55
)

type ModbusCodec struct {
	Head *HeadCodec
	Body []byte
	Tail *TailCodec
}

type Codec interface {
	Marshal() []byte
	MsgId() uint64
	HeaderLength() uint32
	GetLength() uint32
	SetData(buf []byte)
}

type Parser interface {
	CheckHeader([]byte) (Codec, error)
}

// Codec format
// [head] [version] [cseq] [type] [dev id] [length] [data] [crc] [tail]
//
// [1字节包头][1字节版本号][2字节请求seq][1字节数据类型][1字节设备ID][4字节数据长度][数据][4字节CRC校验码][1字节包尾]
