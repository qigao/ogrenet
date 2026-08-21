//go:build windows

package transport

import (
	"errors"
	"syscall"
)

const (
	wsaConnRefused syscall.Errno = 10061
	wsaConnReset   syscall.Errno = 10054
	wsaConnAborted syscall.Errno = 10053
	wsaShutdown    syscall.Errno = 10058
	wsaNotConn     syscall.Errno = 10057
)

func classifyPlatformCause(op Op, err error) (ErrorKind, error, bool) {
	switch {
	case errors.Is(err, wsaConnRefused):
		return ErrorRefused, ErrConnectionRefused, true
	case errors.Is(err, wsaConnReset), errors.Is(err, wsaConnAborted):
		return ErrorReset, ErrConnectionReset, true
	case errors.Is(err, wsaShutdown):
		return ErrorPeerClosed, ErrPeerClosed, true
	case errors.Is(err, wsaNotConn) && establishedIOOp(op):
		return ErrorPeerClosed, ErrPeerClosed, true
	default:
		return ErrorUnknown, nil, false
	}
}
