package transport

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"testing"

	"github.com/coder/websocket"
	"github.com/qigao/ogrenet"
)

func TestClassifyOperationalKnownSentinels(t *testing.T) {
	cases := []struct {
		name     string
		op       Op
		cause    error
		kind     ErrorKind
		specific error
		broad    error
	}{
		{"closed", OpSend, ErrClosed, ErrorClosed, ErrClosed, nil},
		{"would-block", OpSend, ErrWouldBlock, ErrorBackpressure, ErrWouldBlock, nil},
		{"message-too-large", OpSend, ErrMessageTooLarge, ErrorTooLarge, ErrMessageTooLarge, nil},
		{"datagram-too-large", OpSend, ErrDatagramTooLarge, ErrorTooLarge, ErrDatagramTooLarge, nil},
		{"frame-budget", OpSend, ErrFrameExceedsQueueBudget, ErrorResourceExhausted, ErrFrameExceedsQueueBudget, ErrResourceExhausted},
		{"read-buffer", OpRead, ErrReadBufferFull, ErrorResourceExhausted, ErrReadBufferFull, ErrResourceExhausted},
		{"invalid-framer-runtime", OpRead, ErrInvalidFramer, ErrorUnknown, ErrInvalidFramer, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyOperational(tc.op, ogrenet.SchemeTCP, nil, nil, tc.cause, hintNone)
			var te *Error
			if !errors.As(err, &te) || te.Kind != tc.kind {
				t.Fatalf("err=%#v", err)
			}
			if !errors.Is(err, tc.specific) {
				t.Fatalf("missing specific sentinel: %v", err)
			}
			if tc.broad != nil && !errors.Is(err, tc.broad) {
				t.Fatalf("missing broad category: %v", err)
			}
		})
	}
}

func TestClassifyOperationalPreservesTimeoutAndLimitTypes(t *testing.T) {
	timeout := &TimeoutError{Kind: TimeoutWrite, Cause: context.DeadlineExceeded}
	terr := classifyOperational(OpWrite, ogrenet.SchemeTCP, nil, nil, timeout, hintNone)
	var te *Error
	var timeoutOut *TimeoutError
	if !errors.As(terr, &te) || te.Kind != ErrorTimeout || !errors.As(terr, &timeoutOut) || timeoutOut.Kind != TimeoutWrite || !errors.Is(terr, ErrTimeout) {
		t.Fatalf("timeout chain=%#v", terr)
	}

	limit := &LimitError{Kind: LimitConnections, Limit: 10}
	lerr := classifyOperational(OpDial, ogrenet.SchemeTCP, nil, nil, limit, hintNone)
	var limitOut *LimitError
	if !errors.As(lerr, &te) || te.Kind != ErrorResourceExhausted || !errors.As(lerr, &limitOut) || !errors.Is(lerr, ErrResourceExhausted) {
		t.Fatalf("limit chain=%#v", lerr)
	}
}

func TestClassifyOperationalDNS(t *testing.T) {
	raw := &net.DNSError{Name: "does-not-exist.invalid", Err: "no such host"}
	err := classifyOperational(OpDial, ogrenet.SchemeTCP, nil, nil, raw, hintNone)
	var te *Error
	var dns *net.DNSError
	if !errors.As(err, &te) || te.Kind != ErrorDNS || !errors.Is(err, ErrDNS) || !errors.As(err, &dns) {
		t.Fatalf("dns chain=%#v", err)
	}
}

func TestClassifyTLSCertificateFailure(t *testing.T) {
	raw := x509.UnknownAuthorityError{Cert: &x509.Certificate{}}
	err := classifyOperational(OpHandshake, ogrenet.SchemeTLS, nil, nil, raw, hintTLSHandshake)
	var te *Error
	var unknownAuthority x509.UnknownAuthorityError
	if !errors.As(err, &te) || te.Kind != ErrorTLS || !errors.Is(err, ErrTLS) || !errors.As(err, &unknownAuthority) {
		t.Fatalf("tls chain=%#v", err)
	}
}

func TestClassifyOperationalWireHint(t *testing.T) {
	raw := errors.New("bad remote frame")
	err := classifyOperational(OpRead, ogrenet.SchemeTCP, nil, nil, raw, hintWireDecode)
	var te *Error
	if !errors.As(err, &te) || te.Kind != ErrorProtocol || !errors.Is(err, ErrProtocolViolation) || !errors.Is(err, raw) {
		t.Fatalf("wire chain=%#v", err)
	}
}

func TestClassifyOperationalUnknownPreservesCause(t *testing.T) {
	raw := errors.New("mystery")
	err := classifyOperational(OpRead, ogrenet.SchemeTCP, nil, nil, raw, hintNone)
	var te *Error
	if !errors.As(err, &te) || te.Kind != ErrorUnknown || !errors.Is(err, raw) {
		t.Fatalf("unknown chain=%#v", err)
	}
}

func TestClassifyOperationalDoesNotRewrapError(t *testing.T) {
	first := envelopeOperational(OpRead, ogrenet.SchemeTCP, nil, nil, ErrorUnknown, errors.New("raw"))
	second := classifyOperational(OpWrite, ogrenet.SchemeTCP, nil, nil, first, hintNone)
	if second != first {
		t.Fatalf("classifier rewrapped existing error: %p != %p", second, first)
	}
}

func TestClassifyWSStatus(t *testing.T) {
	cases := []struct {
		status   websocket.StatusCode
		kind     ErrorKind
		category error
		ok       bool
	}{
		{websocket.StatusNormalClosure, ErrorUnknown, nil, false},
		{websocket.StatusGoingAway, ErrorUnknown, nil, false},
		{websocket.StatusProtocolError, ErrorProtocol, ErrProtocolViolation, true},
		{websocket.StatusUnsupportedData, ErrorProtocol, ErrProtocolViolation, true},
		{websocket.StatusInvalidFramePayloadData, ErrorProtocol, ErrProtocolViolation, true},
		{websocket.StatusPolicyViolation, ErrorProtocol, ErrProtocolViolation, true},
		{websocket.StatusMessageTooBig, ErrorTooLarge, ErrMessageTooLarge, true},
		{websocket.StatusInternalError, ErrorUnknown, nil, true},
	}
	for _, tc := range cases {
		kind, category, ok := classifyWSStatus(tc.status)
		if kind != tc.kind || category != tc.category || ok != tc.ok {
			t.Fatalf("status %d => (%v,%v,%v), want (%v,%v,%v)", tc.status, kind, category, ok, tc.kind, tc.category, tc.ok)
		}
	}
}
