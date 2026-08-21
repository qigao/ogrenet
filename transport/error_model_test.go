package transport

import (
	"errors"
	"net"
	"testing"

	"github.com/qigao/ogrenet"
)

func TestTransportErrorEnvelopeContract(t *testing.T) {
	raw := errors.New("raw-reset")
	cause := categorized(ErrConnectionReset, raw)
	err := envelopeOperational(
		OpRead,
		ogrenet.SchemeTCP,
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10001},
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10002},
		ErrorReset,
		cause,
	)

	var te *Error
	if !errors.As(err, &te) {
		t.Fatalf("errors.As(*Error) = false: %T %v", err, err)
	}
	if te.Op != OpRead || te.Protocol != ogrenet.SchemeTCP || te.Kind != ErrorReset {
		t.Fatalf("envelope = %+v", te)
	}
	if !errors.Is(err, ErrConnectionReset) {
		t.Fatalf("errors.Is(ErrConnectionReset) = false: %v", err)
	}
	if !errors.Is(err, raw) {
		t.Fatalf("raw cause not reachable: %v", err)
	}
}

func TestOpString(t *testing.T) {
	tests := []struct {
		op   Op
		want string
	}{
		{OpDial, "dial"},
		{OpListen, "listen"},
		{OpAccept, "accept"},
		{OpHandshake, "handshake"},
		{OpUpgrade, "upgrade"},
		{OpRead, "read"},
		{OpWrite, "write"},
		{OpSend, "send"},
		{OpReceive, "receive"},
		{OpClose, "close"},
		{OpShutdown, "shutdown"},
		{Op(255), "unknown"},
	}
	for _, tc := range tests {
		if got := tc.op.String(); got != tc.want {
			t.Fatalf("%d.String() = %q, want %q", tc.op, got, tc.want)
		}
	}
}

func TestErrorKindString(t *testing.T) {
	tests := []struct {
		kind ErrorKind
		want string
	}{
		{ErrorUnknown, "unknown"},
		{ErrorClosed, "closed"},
		{ErrorPeerClosed, "peer-closed"},
		{ErrorTimeout, "timeout"},
		{ErrorRefused, "refused"},
		{ErrorReset, "reset"},
		{ErrorDNS, "dns"},
		{ErrorTLS, "tls"},
		{ErrorProtocol, "protocol"},
		{ErrorResourceExhausted, "resource-exhausted"},
		{ErrorBackpressure, "backpressure"},
		{ErrorTooLarge, "too-large"},
		{ErrorKind(255), "unknown"},
	}
	for _, tc := range tests {
		if got := tc.kind.String(); got != tc.want {
			t.Fatalf("%d.String() = %q, want %q", tc.kind, got, tc.want)
		}
	}
}

func TestEnvelopeOperationalDoesNotDoubleWrap(t *testing.T) {
	first := envelopeOperational(OpRead, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, errors.New("raw"))
	second := envelopeOperational(OpWrite, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, first)
	if second != first {
		t.Fatalf("double wrap changed identity: %p != %p", second, first)
	}
}

func TestEnvelopeOperationalNilStaysNil(t *testing.T) {
	if got := envelopeOperational(OpRead, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, nil); got != nil {
		t.Fatalf("nil cause = %v, want nil", got)
	}
}

func TestTransportErrorSnapshotsTCPAddresses(t *testing.T) {
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1001}
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 1002}
	err := envelopeOperational(OpRead, ogrenet.SchemeTCP, local, remote, ErrorUnknown, errors.New("raw"))
	local.IP[3] = 9
	remote.IP[3] = 9

	var te *Error
	if !errors.As(err, &te) {
		t.Fatal("missing *Error")
	}
	if got := te.Local.String(); got != "127.0.0.1:1001" {
		t.Fatalf("local snapshot = %q", got)
	}
	if got := te.Remote.String(); got != "127.0.0.2:1002" {
		t.Fatalf("remote snapshot = %q", got)
	}
}

func TestTransportErrorUnknownPreservesRawCause(t *testing.T) {
	raw := errors.New("mystery")
	err := envelopeOperational(OpRead, ogrenet.SchemeWS, nil, nil, ErrorUnknown, raw)

	var te *Error
	if !errors.As(err, &te) {
		t.Fatal("missing *Error")
	}
	if te.Kind != ErrorUnknown {
		t.Fatalf("Kind = %v, want ErrorUnknown", te.Kind)
	}
	if !errors.Is(err, raw) {
		t.Fatalf("raw cause not reachable: %v", err)
	}
}
