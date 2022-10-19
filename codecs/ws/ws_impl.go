package ws

import "github.com/qigao/ogrenet/network"

type Codec struct {
	Conn        *network.Conn
	MessageType int
	Content     []byte
}
