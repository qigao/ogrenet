//go:build !windows

package transport

import (
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
)

func TestClassifyPlatformCauseUnix(t *testing.T) {
	cases := []struct {
		name     string
		op       Op
		errno    syscall.Errno
		kind     ErrorKind
		category error
		ok       bool
	}{
		{"refused", OpDial, syscall.ECONNREFUSED, ErrorRefused, ErrConnectionRefused, true},
		{"reset", OpRead, syscall.ECONNRESET, ErrorReset, ErrConnectionReset, true},
		{"pipe", OpWrite, syscall.EPIPE, ErrorPeerClosed, ErrPeerClosed, true},
		{"not-connected-established", OpWrite, syscall.ENOTCONN, ErrorPeerClosed, ErrPeerClosed, true},
		{"not-connected-dial", OpDial, syscall.ENOTCONN, ErrorUnknown, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := &net.OpError{Op: tc.op.String(), Net: "tcp", Err: &os.SyscallError{Syscall: tc.op.String(), Err: tc.errno}}
			kind, category, ok := classifyPlatformCause(tc.op, raw)
			if kind != tc.kind || category != tc.category || ok != tc.ok {
				t.Fatalf("classifyPlatformCause = (%v,%v,%v), want (%v,%v,%v)", kind, category, ok, tc.kind, tc.category, tc.ok)
			}
			if tc.ok && !errors.Is(raw, tc.errno) {
				t.Fatalf("raw errno not reachable: %v", raw)
			}
		})
	}
}
