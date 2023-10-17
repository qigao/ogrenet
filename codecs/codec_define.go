package codecs

type Codec interface {
	Encode() ([]byte, error)
	Decode(buf []byte) error
	Length() int
}
