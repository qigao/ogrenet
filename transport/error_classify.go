package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"

	"github.com/coder/websocket"
	"github.com/qigao/ogrenet"
)

type classifyHint uint8

const (
	hintNone classifyHint = iota
	hintTLSHandshake
	hintWSUpgrade
	hintWireDecode
	hintMessageDecode
)

func classifyOperational(op Op, protocol ogrenet.Scheme, local, remote net.Addr, cause error, hint classifyHint) error {
	if cause == nil {
		return nil
	}
	var existing *Error
	if errors.As(cause, &existing) {
		return cause
	}

	var timeout *TimeoutError
	if errors.As(cause, &timeout) {
		return envelopeOperational(op, protocol, local, remote, ErrorTimeout, cause)
	}
	if errors.Is(cause, ErrTimeout) {
		return envelopeOperational(op, protocol, local, remote, ErrorTimeout, cause)
	}
	var netErr net.Error
	if errors.As(cause, &netErr) && netErr.Timeout() {
		return envelopeOperational(op, protocol, local, remote, ErrorTimeout, categorized(ErrTimeout, cause))
	}

	var limit *LimitError
	if errors.As(cause, &limit) || errors.Is(cause, ErrResourceExhausted) {
		return envelopeOperational(op, protocol, local, remote, ErrorResourceExhausted, cause)
	}

	switch {
	case errors.Is(cause, ErrClosed):
		return envelopeOperational(op, protocol, local, remote, ErrorClosed, cause)
	case errors.Is(cause, ErrWouldBlock):
		return envelopeOperational(op, protocol, local, remote, ErrorBackpressure, cause)
	case errors.Is(cause, ErrMessageTooLarge), errors.Is(cause, ErrDatagramTooLarge):
		return envelopeOperational(op, protocol, local, remote, ErrorTooLarge, cause)
	case errors.Is(cause, ErrFrameExceedsQueueBudget), errors.Is(cause, ErrReadBufferFull):
		return envelopeOperational(op, protocol, local, remote, ErrorResourceExhausted, categorized(ErrResourceExhausted, cause))
	case errors.Is(cause, ErrInvalidFramer):
		return envelopeOperational(op, protocol, local, remote, ErrorUnknown, cause)
	}

	var dns *net.DNSError
	if errors.As(cause, &dns) {
		return envelopeOperational(op, protocol, local, remote, ErrorDNS, categorized(ErrDNS, cause))
	}

	if kind, category, ok := classifyPlatformCause(op, cause); ok {
		return envelopeOperational(op, protocol, local, remote, kind, categorized(category, cause))
	}

	if isTLSCause(cause) || hint == hintTLSHandshake {
		return envelopeOperational(op, protocol, local, remote, ErrorTLS, categorized(ErrTLS, cause))
	}

	if status := websocket.CloseStatus(cause); status != -1 {
		if kind, category, ok := classifyWSStatus(status); ok {
			if category != nil {
				cause = categorized(category, cause)
			}
			return envelopeOperational(op, protocol, local, remote, kind, cause)
		}
	}

	if hint == hintWireDecode || hint == hintMessageDecode || hint == hintWSUpgrade {
		return envelopeOperational(op, protocol, local, remote, ErrorProtocol, categorized(ErrProtocolViolation, cause))
	}

	return envelopeOperational(op, protocol, local, remote, ErrorUnknown, cause)
}

func isTLSCause(err error) bool {
	var verification *tls.CertificateVerificationError
	if errors.As(err, &verification) {
		return true
	}
	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthority) {
		return true
	}
	var hostname x509.HostnameError
	if errors.As(err, &hostname) {
		return true
	}
	var invalid x509.CertificateInvalidError
	return errors.As(err, &invalid)
}

func classifyWSStatus(status websocket.StatusCode) (ErrorKind, error, bool) {
	switch status {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return ErrorUnknown, nil, false
	case websocket.StatusProtocolError,
		websocket.StatusUnsupportedData,
		websocket.StatusInvalidFramePayloadData,
		websocket.StatusPolicyViolation:
		return ErrorProtocol, ErrProtocolViolation, true
	case websocket.StatusMessageTooBig:
		return ErrorTooLarge, ErrMessageTooLarge, true
	case websocket.StatusInternalError:
		return ErrorUnknown, nil, true
	default:
		return ErrorUnknown, nil, false
	}
}

func establishedIOOp(op Op) bool {
	switch op {
	case OpRead, OpWrite, OpSend, OpReceive, OpClose:
		return true
	default:
		return false
	}
}
