//go:build !windows

package transport

import (
	"errors"
	"syscall"
)

func classifyPlatformCause(op Op, err error) (ErrorKind, error, bool) {
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return ErrorRefused, ErrConnectionRefused, true
	case errors.Is(err, syscall.ECONNRESET):
		return ErrorReset, ErrConnectionReset, true
	case errors.Is(err, syscall.EPIPE):
		return ErrorPeerClosed, ErrPeerClosed, true
	case errors.Is(err, syscall.ENOTCONN) && establishedIOOp(op):
		return ErrorPeerClosed, ErrPeerClosed, true
	default:
		return ErrorUnknown, nil, false
	}
}
