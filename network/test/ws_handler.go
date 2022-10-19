package main

import (
	"fmt"

	"github.com/qigao/ogrenet/network"
)

type Ws struct{}

func (Ws) OnConnect(c *network.Conn) {
	fmt.Println("connect:", c.Fd())
}

func (Ws) OnMessage(c *network.Conn, bytes []byte) {
	fmt.Println("read:", string(bytes))
	c.Write(bytes)
}

func (Ws) OnClose(c *network.Conn) {
	fmt.Println("close:", c.Fd())
}
