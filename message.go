package ogrenet

import (
	"errors"
	"unicode/utf8"
)

type PayloadType uint8

const (
	PayloadBinary PayloadType = 0x00
	PayloadText   PayloadType = 0x01
)

var (
	ErrInvalidPayloadType = errors.New("ogrenet: invalid payload type")
	ErrInvalidUTF8        = errors.New("ogrenet: text payload is not valid UTF-8")
)

type Message struct {
	Type PayloadType
	Data []byte
}

func Text(s string) Message { return Message{Type: PayloadText, Data: []byte(s)} }
func Bin(b []byte) Message  { return Message{Type: PayloadBinary, Data: append([]byte(nil), b...)} }
func (m Message) Validate() error {
	switch m.Type {
	case PayloadBinary:
		return nil
	case PayloadText:
		if !utf8.Valid(m.Data) {
			return ErrInvalidUTF8
		}
		return nil
	default:
		return ErrInvalidPayloadType
	}
}
