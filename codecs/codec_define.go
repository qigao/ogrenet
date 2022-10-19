package codecs

const (
	DefaultMagicHead = 0xAA
	DefaultMagicTail = 0x55
)

type Codec interface {
	Encode() ([]byte, error)
	Decode(buf []byte) error
	Length() uint16
}

// Codec format
// [head] [version] [cseq] [type] [dev id] [length] [data] [crc] [tail]
//
// [1字节包头][1字节版本号][2字节请求seq][1字节数据类型][1字节设备ID][4字节数据长度][数据][4字节CRC校验码][1字节包尾]
