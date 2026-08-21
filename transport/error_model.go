package transport

import (
	"errors"
	"fmt"
	"net"

	"github.com/qigao/ogrenet"
)

type Op uint8

const (
	OpDial Op = iota + 1
	OpListen
	OpAccept
	OpHandshake
	OpUpgrade
	OpRead
	OpWrite
	OpSend
	OpReceive
	OpClose
	OpShutdown
)

func (o Op) String() string {
	switch o {
	case OpDial:
		return "dial"
	case OpListen:
		return "listen"
	case OpAccept:
		return "accept"
	case OpHandshake:
		return "handshake"
	case OpUpgrade:
		return "upgrade"
	case OpRead:
		return "read"
	case OpWrite:
		return "write"
	case OpSend:
		return "send"
	case OpReceive:
		return "receive"
	case OpClose:
		return "close"
	case OpShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

type ErrorKind uint8

const (
	ErrorUnknown ErrorKind = iota
	ErrorClosed
	ErrorPeerClosed
	ErrorTimeout
	ErrorRefused
	ErrorReset
	ErrorDNS
	ErrorTLS
	ErrorProtocol
	ErrorResourceExhausted
	ErrorBackpressure
	ErrorTooLarge
)

func (k ErrorKind) String() string {
	switch k {
	case ErrorClosed:
		return "closed"
	case ErrorPeerClosed:
		return "peer-closed"
	case ErrorTimeout:
		return "timeout"
	case ErrorRefused:
		return "refused"
	case ErrorReset:
		return "reset"
	case ErrorDNS:
		return "dns"
	case ErrorTLS:
		return "tls"
	case ErrorProtocol:
		return "protocol"
	case ErrorResourceExhausted:
		return "resource-exhausted"
	case ErrorBackpressure:
		return "backpressure"
	case ErrorTooLarge:
		return "too-large"
	default:
		return "unknown"
	}
}

type Error struct {
	Op       Op
	Protocol ogrenet.Scheme
	Kind     ErrorKind
	Local    net.Addr
	Remote   net.Addr
	Cause    error
}

func (e *Error) Error() string {
	if e == nil {
		return "transport: operational error"
	}
	prefix := fmt.Sprintf("transport: %s %s %s", e.Protocol, e.Op, e.Kind)
	if e.Cause == nil {
		return prefix
	}
	return prefix + ": " + e.Cause.Error()
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type categorizedCause struct {
	category error
	cause    error
}

func (e *categorizedCause) Error() string {
	if e == nil || e.category == nil {
		return "transport: categorized failure"
	}
	if e.cause == nil {
		return e.category.Error()
	}
	return e.category.Error() + ": " + e.cause.Error()
}

func (e *categorizedCause) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *categorizedCause) Is(target error) bool {
	return e != nil && target == e.category
}

func categorized(category, cause error) error {
	if cause == nil {
		return category
	}
	if errors.Is(cause, category) {
		return cause
	}
	return &categorizedCause{category: category, cause: cause}
}

func envelopeOperational(op Op, protocol ogrenet.Scheme, local, remote net.Addr, kind ErrorKind, cause error) error {
	if cause == nil {
		return nil
	}
	var existing *Error
	if errors.As(cause, &existing) {
		return cause
	}
	return &Error{
		Op:       op,
		Protocol: protocol,
		Kind:     kind,
		Local:    snapshotAddr(local),
		Remote:   snapshotAddr(remote),
		Cause:    cause,
	}
}

func snapshotAddr(addr net.Addr) net.Addr {
	switch a := addr.(type) {
	case nil:
		return nil
	case *net.TCPAddr:
		if a == nil {
			return nil
		}
		copyAddr := *a
		copyAddr.IP = append(net.IP(nil), a.IP...)
		return &copyAddr
	case *net.UDPAddr:
		if a == nil {
			return nil
		}
		copyAddr := *a
		copyAddr.IP = append(net.IP(nil), a.IP...)
		return &copyAddr
	case staticAddr:
		return a
	case *staticAddr:
		if a == nil {
			return nil
		}
		copyAddr := *a
		return &copyAddr
	default:
		return addr
	}
}
