package codecs

type ModBus struct {
	Head *Head
	Body []byte
	Tail *Tail
}

type Head struct {
	Magic   uint8
	Version uint8
	Cseq    CSEQ
	Type    uint8
	Port    uint8
	BodyLen uint16
}

type Tail struct {
	CRC   uint16
	Magic uint8
}

// Codec format
// [head] [version] [cseq] [type] [port id] [length] [data] [crc] [tail]
//
// [1字节包头][1字节版本号][4字节请求seq][1字节数据类型][1字节设备ID][4字节数据长度][数据][4字节CRC校验码][1字节包尾]
