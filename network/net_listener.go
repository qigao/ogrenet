package network

import (
	"net"
	"sync"
)

type NetListener struct {
	once     sync.Once
	listener net.Listener
}

func (ln *NetListener) Close() error {
	ln.once.Do(func() {
		if ln.listener != nil {
			ln.listener.Close()
		}
	})
	return nil
}

func (ln *NetListener) Accept() (net.Conn, error) {
	conn, err := ln.listener.Accept()
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, nil
	}

	ec, ok := conn.(Conn)

	if !ok {
		ec, err = dupStdConn(conn)
	}
	return ec, err
}
