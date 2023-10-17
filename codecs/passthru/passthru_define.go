package passthru

type CodecPassThru struct {
	Head *HeadCodec
	Body []byte
	Tail *TailCodec
}

type HeadCodec struct {
	Magic     uint8
	Version   uint8
	Cseq      [4]byte
	CodecType CodecType
	ID        [4]byte
	BodyLen   uint16
}

type TailCodec struct {
	CRC   uint16
	Magic uint8
}

type CodecType uint8

const (
	Register CodecType = iota
	Unregister
	HeartBeat
	Data
	Ack
	Close
	Error
	ReConnect
)

const (
	version uint8 = 0x01
)

// Codec format
// [head] [version] [cseq] [Codec type] [client id] [length] [data] [crc] [tail]
//
// [1字节包头][1字节版本号][4字节请求seq][1字节数据类型][1字节设备ID][4字节数据长度][数据][4字节CRC校验码][1字节包尾]
