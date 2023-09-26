package errors

import "github.com/pkg/errors"

var (
	ErrIncompletePacket    = errors.New("incomplete packet")
	ErrInvalidMagicNumber  = errors.New("invalid Magic number")
	ErrInvalidCRCValue     = errors.New("invalid crc16 checksum")
	ErrInvalidCodecHead    = errors.New("invalid codecs head")
	ErrInvalidCodecTail    = errors.New("invalid codecs tail")
	ErrBufferInvalidIsNil  = errors.New("buffer: invalid is nil ")
	ErrBufferInvalidStart  = errors.New("buffer: invalid start byte")
	ErrBufferInvalidHeader = errors.New("buffer: invalid header")
	ErrBufferDataTooLong   = errors.New("buffer: data too long bytes")
)
