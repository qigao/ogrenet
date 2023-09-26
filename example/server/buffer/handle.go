package main

import (
	"fmt"
	codecs2 "github.com/qigao/ogrenet/src/codecs"
	"github.com/qigao/ogrenet/src/network"

	example "github.com/qigao/ogrenet/example/codec"
)

type (
	Handle struct{}
	conn   struct {
		c     *network.Connection
		codec *codecs2.Message
	}
)

func (c *conn) onMessage(m codecs2.Codec) {
	msg := m.(*example.ExampleCodec)
	msg.SetData([]byte("recv msg"))
	_, _ = c.c.Write(msg.Marshal())
}

// OnConnect 当TCP长连接建立成功是回调
func (h *Handle) OnConnect(c *network.Connection) {
	fmt.Println("new connection : ", c.RemoteAddr())
	buffer := codecs2.NewBuffer(example.CheckHeader)
	buffer.OnMessage((&conn{c: c}).onMessage)
	c.SetBuffer(buffer)
}

// OnMessage 当客户端有数据写入是回调
func (h *Handle) OnMessage(c *network.Connection, bytes []byte) {
	c.Write(bytes)
}

// OnClose 当客户端主动断开链接或者超时时回调,err返回关闭的原因
func (h *Handle) OnClose(c *network.Connection, msg string) {
	fmt.Println("close connection : ", c.RemoteAddr(), msg)
}
