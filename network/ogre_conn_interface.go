package network

import "net"

type OgreConn interface {
	net.Conn
	Fd() int
	Context() interface{}
	SetContext(context interface{})
}
