//go:build windows

package network

import (
	"net"
	"syscall"
)

func ReadConn(syscallConn syscall.RawConn, buf []byte) (n int, err error) {
	return 0, nil
}

// 监听可重用的端口
func ListenReuseAddr(network string, addr string) (net.Listener, error) {
	return nil, nil
}

func SetLimit() {
}
