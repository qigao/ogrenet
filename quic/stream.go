package quic

import (
	"io"

	quicgo "github.com/quic-go/quic-go"
)

// Stream is one bidirectional QUIC stream.
//
// CloseWrite sends FIN for the local write direction. Reads remain valid until
// the peer sends its FIN or resets the stream.
type Stream struct {
	raw *quicgo.Stream
}

func (s *Stream) Read(p []byte) (int, error) {
	n, err := s.raw.Read(p)
	if err == nil || err == io.EOF {
		return n, err
	}
	return n, wrapError(OpReadStream, err)
}

func (s *Stream) Write(p []byte) (int, error) {
	n, err := s.raw.Write(p)
	if err != nil {
		return n, wrapError(OpWriteStream, err)
	}
	return n, nil
}

func (s *Stream) StreamID() uint64 { return uint64(s.raw.StreamID()) }

func (s *Stream) CancelRead(code uint64) {
	s.raw.CancelRead(quicgo.StreamErrorCode(code))
}

func (s *Stream) CancelWrite(code uint64) {
	s.raw.CancelWrite(quicgo.StreamErrorCode(code))
}

func (s *Stream) CloseWrite() error {
	return wrapError(OpCloseStream, s.raw.Close())
}
