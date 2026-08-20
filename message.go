package ogrenet

import (
	"errors"
	"unicode/utf8"
)

// PayloadType identifies the application payload carried by a Message.
type PayloadType uint8

const (
	PayloadBinary PayloadType = iota
	PayloadText
)

var (
	ErrInvalidPayloadType = errors.New("ogrenet: invalid payload type")
	ErrInvalidUTF8        = errors.New("ogrenet: text payload is not valid UTF-8")
)

// Message is the transport-level application message.
// Data is always plaintext at the Handler boundary.
type Message struct {
	Type PayloadType
	Data []byte
}

// Text creates a UTF-8 text message.
func Text(s string) Message {
	return Message{Type: PayloadText, Data: []byte(s)}
}

// Bin creates a binary message and copies b so callers can safely reuse it.
func Bin(b []byte) Message {
	return Message{Type: PayloadBinary, Data: append([]byte(nil), b...)}
}

// Validate checks the payload type and UTF-8 requirement for text messages.
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
