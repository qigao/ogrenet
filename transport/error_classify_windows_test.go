//go:build windows

package transport

import (
	"net"
	"os"
	"syscall"
	"testing"
)

func TestClassifyPlatformCauseWindows(t *testing.T) {
	const (
		wsaConnRefused syscall.Errno = 10061
		wsaConnReset   syscall.Errno = 10054
		wsaConnAborted syscall.Errno = 10053
		wsaShutdown    syscall.Errno = 10058
		wsaNotConn     syscall.Errno = 10057
	)
	cases := []struct {
		name     string
		op       Op
		errno    syscall.Errno
		kind     ErrorKind
		category error
		ok       bool
	}{
		{"refused", OpDial, wsaConnRefused, ErrorRefused, ErrConnectionRefused, true},
		{"reset", OpRead, wsaConnReset, ErrorReset, ErrConnectionReset, true},
		{"aborted", OpWrite, wsaConnAborted, ErrorReset, ErrConnectionReset, true},
		{"shutdown", OpWrite, wsaShutdown, ErrorPeerClosed, ErrPeerClosed, true},
		{"not-connected-established", OpWrite, wsaNotConn, ErrorPeerClosed, ErrPeerClosed, true},
		{"not-connected-dial", OpDial, wsaNotConn, ErrorUnknown, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := &net.OpError{Op: tc.op.String(), Net: "tcp", Err: &os.SyscallError{Syscall: tc.op.String(), Err: tc.errno}}
			kind, category, ok := classifyPlatformCause(tc.op, raw)
			if kind != tc.kind || category != tc.category || ok != tc.ok {
				t.Fatalf("classifyPlatformCause = (%v,%v,%v), want (%v,%v,%v)", kind, category, ok, tc.kind, tc.category, tc.ok)
			}
		})
	}
}
