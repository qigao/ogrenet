package quic

import (
	"context"
	"errors"
	"fmt"
	"net"

	quicgo "github.com/quic-go/quic-go"
)

// Op identifies a QUIC operation for machine-readable errors.
type Op string

const (
	OpDial            Op = "dial"
	OpOpenStream      Op = "open_stream"
	OpAcceptStream    Op = "accept_stream"
	OpReadStream      Op = "read_stream"
	OpWriteStream     Op = "write_stream"
	OpCloseStream     Op = "close_stream"
	OpSendDatagram    Op = "send_datagram"
	OpReceiveDatagram Op = "receive_datagram"
	OpClose           Op = "close"
)

// ErrorKind is a stable, dependency-independent classification of QUIC errors.
type ErrorKind uint8

const (
	ErrorUnknown ErrorKind = iota
	ErrorCanceled
	ErrorTimeout
	ErrorClosed
	ErrorProtocol
	ErrorResourceExhausted
	ErrorMessageTooLarge
)

func (k ErrorKind) String() string {
	switch k {
	case ErrorCanceled:
		return "canceled"
	case ErrorTimeout:
		return "timeout"
	case ErrorClosed:
		return "closed"
	case ErrorProtocol:
		return "protocol"
	case ErrorResourceExhausted:
		return "resource_exhausted"
	case ErrorMessageTooLarge:
		return "message_too_large"
	default:
		return "unknown"
	}
}

var (
	ErrCanceled          = errors.New("quic: operation canceled")
	ErrTimeout           = errors.New("quic: timeout")
	ErrClosed            = errors.New("quic: closed")
	ErrProtocol          = errors.New("quic: protocol error")
	ErrResourceExhausted = errors.New("quic: resource exhausted")
	ErrMessageTooLarge   = errors.New("quic: message too large")
	ErrDatagramsDisabled = errors.New("quic: datagrams are disabled")
)

// Error wraps a QUIC failure without exposing quic-go-specific error types as
// the public classification contract. Cause is preserved for errors.Is/As.
type Error struct {
	Op    Op
	Kind  ErrorKind
	Cause error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Cause == nil {
		return fmt.Sprintf("quic: %s: %s", e.Op, e.Kind)
	}
	return fmt.Sprintf("quic: %s: %s: %v", e.Op, e.Kind, e.Cause)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}
	switch target {
	case ErrCanceled:
		return e.Kind == ErrorCanceled
	case ErrTimeout:
		return e.Kind == ErrorTimeout
	case ErrClosed:
		return e.Kind == ErrorClosed
	case ErrProtocol:
		return e.Kind == ErrorProtocol
	case ErrResourceExhausted:
		return e.Kind == ErrorResourceExhausted
	case ErrMessageTooLarge:
		return e.Kind == ErrorMessageTooLarge
	default:
		return false
	}
}

// Timeout allows ErrorTimeout to participate in net.Error checks.
func (e *Error) Timeout() bool { return e != nil && e.Kind == ErrorTimeout }

func wrapError(op Op, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Op: op, Kind: classifyError(err), Cause: err}
}

func classifyError(err error) ErrorKind {
	if err == nil {
		return ErrorUnknown
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorTimeout
	}
	if errors.Is(err, context.Canceled) {
		return ErrorCanceled
	}
	if errors.Is(err, net.ErrClosed) {
		return ErrorClosed
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrorTimeout
	}

	var datagramTooLarge *quicgo.DatagramTooLargeError
	if errors.As(err, &datagramTooLarge) {
		return ErrorMessageTooLarge
	}
	var streamLimit quicgo.StreamLimitReachedError
	if errors.As(err, &streamLimit) {
		return ErrorResourceExhausted
	}
	var appErr *quicgo.ApplicationError
	if errors.As(err, &appErr) {
		return ErrorClosed
	}
	var streamErr *quicgo.StreamError
	if errors.As(err, &streamErr) {
		return ErrorClosed
	}
	var statelessReset *quicgo.StatelessResetError
	if errors.As(err, &statelessReset) {
		return ErrorClosed
	}
	var transportErr *quicgo.TransportError
	if errors.As(err, &transportErr) {
		return ErrorProtocol
	}
	var versionErr *quicgo.VersionNegotiationError
	if errors.As(err, &versionErr) {
		return ErrorProtocol
	}
	return ErrorUnknown
}
