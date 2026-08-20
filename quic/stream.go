package quic

import quicgo "github.com/quic-go/quic-go"

// Stream is one bidirectional QUIC stream.
//
// CloseWrite sends FIN for the local write direction. Reads remain valid until
// the peer sends its FIN or resets the stream.
type Stream struct {
	raw *quicgo.Stream
}

func (s *Stream) Read(p []byte) (int, error)  { return s.raw.Read(p) }
func (s *Stream) Write(p []byte) (int, error) { return s.raw.Write(p) }
func (s *Stream) CloseWrite() error           { return s.raw.Close() }
