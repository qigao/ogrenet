package quic

import (
	"context"
	"errors"
	"testing"

	quicgo "github.com/quic-go/quic-go"
)

func TestErrorClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		kind ErrorKind
		is   error
	}{
		{"canceled", context.Canceled, ErrorCanceled, ErrCanceled},
		{"deadline", context.DeadlineExceeded, ErrorTimeout, ErrTimeout},
		{"datagram", &quicgo.DatagramTooLargeError{}, ErrorMessageTooLarge, ErrMessageTooLarge},
		{"stream limit", quicgo.StreamLimitReachedError{}, ErrorResourceExhausted, ErrResourceExhausted},
		{"transport", &quicgo.TransportError{}, ErrorProtocol, ErrProtocol},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wrapError(OpDial, tt.err)
			var qerr *Error
			if !errors.As(err, &qerr) {
				t.Fatalf("%T is not *Error", err)
			}
			if qerr.Kind != tt.kind {
				t.Fatalf("Kind = %v, want %v", qerr.Kind, tt.kind)
			}
			if !errors.Is(err, tt.is) {
				t.Fatalf("errors.Is(%v) = false", tt.is)
			}
			if !errors.Is(err, tt.err) {
				t.Fatalf("root cause was not preserved")
			}
		})
	}
}
