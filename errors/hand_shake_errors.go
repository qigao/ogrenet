package errors

// HandshakeError describes an error with the handshake from the peer.
type HandshakeError struct {
	Message string
}

func (e HandshakeError) Error() string { return e.Message }
