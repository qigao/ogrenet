package main

import (
	"fmt"
	"github.com/qigao/ogrenet/codecs"
	"github.com/qigao/ogrenet/network"

	example "github.com/qigao/ogrenet/example/codec"
)

type Handle struct{}
type conn struct {
	c     *network.Connection
	codec *codecs.Message
}

func (c *conn) onMessage(m codecs.Codec) {
	msg := m.(*example.ExampleCodec)
	msg.SetData([]byte("recv msg"))
	_, _ = c.c.Write(msg.Marshal())
}

// OnConnect 当TCP长连接建立成功是回调
func (h *Handle) OnConnect(c *network.Connection) {
	fmt.Println("new connection : ", c.RemoteAddr())
	buffer := codecs.NewBuffer(example.CheckHeader)
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
