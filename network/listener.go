package network

import (
	"net"
	"sync"
)

type Listener struct {
	once     sync.Once
	listener net.Listener
	addr     net.Addr // local addr
	engine   *Server
}

func (ln *Listener) Close() error {
	ln.once.Do(func() {
		if ln.listener != nil {
			ln.listener.Close()
		}
	})
	return nil
}

func (ln *Listener) Addr() net.Addr {
	return ln.addr
}

func (ln *Listener) Accept() (net.Conn, error) {
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
