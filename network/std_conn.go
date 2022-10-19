//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly

package network

import (
	"context"
	"net"
	"syscall"

	"github.com/qigao/ogrenet/errors"
)

func dupStdConn(netconn net.Conn) (Conn, error) {
	defer netconn.Close()

	sc, ok := netconn.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		return nil, errors.ErrRawConnNotSupported
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return nil, errors.ErrRawConnNotSupported
	}

	var newFd int
	if err = rc.Control(func(fd uintptr) {
		newFd, err = syscall.Dup(int(fd))
	}); err != nil {
		return nil, err
	}

	c := &conn{
		fd:        newFd,
		localAddr: netconn.LocalAddr(),
		rAddr:     netconn.RemoteAddr(),
		ctx:       context.Background(),
	}
	return c, nil
}
