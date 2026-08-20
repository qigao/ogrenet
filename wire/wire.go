package wire

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/secure"
)

const (
	Magic      uint16 = 0x4f47 // "OG"
	Version    uint8  = 1
	HeaderSize        = 10

	FlagText      uint8 = 1 << 0
	FlagEncrypted uint8 = 1 << 1

	DefaultMaxPayload uint32 = 16 << 20
)

// Header is the fixed 10-byte v1 envelope header.
type Header struct {
	Magic     uint16
	Version   uint8
	Flags     uint8
	Algorithm secure.Algorithm
	Length    uint32
}

// Framer converts application Messages to and from a stream framing format.
type Framer interface {
	Encode(msg ogrenet.Message) ([]byte, error)
	DecodeOne(src []byte) (msg ogrenet.Message, consumed int, err error)
}

// Codec implements the default v1 envelope. Encrypted text payloads are base64
// encoded after encryption so their wire payload remains text-safe; encrypted
// binary payloads remain raw ciphertext.
type Codec struct {
	Cipher     secure.Cipher
	MaxPayload uint32
}

func New(cipher secure.Cipher) *Codec {
	return &Codec{Cipher: cipher, MaxPayload: DefaultMaxPayload}
}

func (c *Codec) maxPayload() uint32 {
	if c.MaxPayload == 0 {
		return DefaultMaxPayload
	}
	return c.MaxPayload
}

func (c *Codec) Encode(msg ogrenet.Message) ([]byte, error) {
	if err := msg.Validate(); err != nil {
		return nil, err
	}

	flags := uint8(0)
	if msg.Type == ogrenet.PayloadText {
		flags |= FlagText
	}

	algorithm := secure.AlgNone
	payload := append([]byte(nil), msg.Data...)
	if c.Cipher != nil {
		var err error
		payload, err = c.Cipher.Seal(nil, msg.Data)
		if err != nil {
			return nil, fmt.Errorf("wire: encrypt payload: %w", err)
		}
		algorithm = c.Cipher.Algorithm()
		flags |= FlagEncrypted
		if msg.Type == ogrenet.PayloadText {
			encoded := make([]byte, base64.RawStdEncoding.EncodedLen(len(payload)))
			base64.RawStdEncoding.Encode(encoded, payload)
			payload = encoded
		}
	}

	if uint64(len(payload)) > uint64(c.maxPayload()) || len(payload) > math.MaxUint32 {
		return nil, ErrFrameTooLarge
	}

	frame := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint16(frame[0:2], Magic)
	frame[2] = Version
	frame[3] = flags
	binary.BigEndian.PutUint16(frame[4:6], uint16(algorithm))
	binary.BigEndian.PutUint32(frame[6:10], uint32(len(payload)))
	copy(frame[HeaderSize:], payload)
	return frame, nil
}

func (c *Codec) DecodeOne(src []byte) (ogrenet.Message, int, error) {
	if len(src) < HeaderSize {
		return ogrenet.Message{}, 0, ErrNeedMore
	}
	if binary.BigEndian.Uint16(src[0:2]) != Magic {
		return ogrenet.Message{}, 0, ErrBadMagic
	}
	if src[2] != Version {
		return ogrenet.Message{}, 0, ErrUnsupportedVersion
	}

	flags := src[3]
	algorithm := secure.Algorithm(binary.BigEndian.Uint16(src[4:6]))
	length := binary.BigEndian.Uint32(src[6:10])
	if length > c.maxPayload() {
		return ogrenet.Message{}, 0, ErrFrameTooLarge
	}
	total := HeaderSize + int(length)
	if len(src) < total {
		return ogrenet.Message{}, 0, ErrNeedMore
	}

	payload := append([]byte(nil), src[HeaderSize:total]...)
	kind := ogrenet.PayloadBinary
	if flags&FlagText != 0 {
		kind = ogrenet.PayloadText
	}

	if flags&FlagEncrypted != 0 {
		if c.Cipher == nil {
			return ogrenet.Message{}, 0, ErrMissingCipher
		}
		if c.Cipher.Algorithm() != algorithm {
			return ogrenet.Message{}, 0, ErrAlgorithmMismatch
		}
		if kind == ogrenet.PayloadText {
			decoded := make([]byte, base64.RawStdEncoding.DecodedLen(len(payload)))
			n, err := base64.RawStdEncoding.Decode(decoded, payload)
			if err != nil {
				return ogrenet.Message{}, 0, fmt.Errorf("wire: decode encrypted text: %w", err)
			}
			payload = decoded[:n]
		}
		plaintext, err := c.Cipher.Open(nil, payload)
		if err != nil {
			return ogrenet.Message{}, 0, fmt.Errorf("wire: decrypt payload: %w", err)
		}
		payload = plaintext
	} else if algorithm != secure.AlgNone {
		return ogrenet.Message{}, 0, ErrAlgorithmMismatch
	}

	msg := ogrenet.Message{Type: kind, Data: payload}
	if err := msg.Validate(); err != nil {
		return ogrenet.Message{}, 0, err
	}
	return msg, total, nil
}

// Decode decodes exactly one frame and rejects trailing bytes.
func (c *Codec) Decode(src []byte) (ogrenet.Message, error) {
	msg, n, err := c.DecodeOne(src)
	if err != nil {
		return ogrenet.Message{}, err
	}
	if n != len(src) {
		return ogrenet.Message{}, ErrTrailingData
	}
	return msg, nil
}

// ParseHeader parses a complete v1 header without consuming payload bytes.
func ParseHeader(src []byte) (Header, error) {
	if len(src) < HeaderSize {
		return Header{}, ErrNeedMore
	}
	h := Header{
		Magic:     binary.BigEndian.Uint16(src[0:2]),
		Version:   src[2],
		Flags:     src[3],
		Algorithm: secure.Algorithm(binary.BigEndian.Uint16(src[4:6])),
		Length:    binary.BigEndian.Uint32(src[6:10]),
	}
	if h.Magic != Magic {
		return Header{}, ErrBadMagic
	}
	if h.Version != Version {
		return Header{}, ErrUnsupportedVersion
	}
	return h, nil
}
