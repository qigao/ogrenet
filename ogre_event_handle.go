package ogrenet

// EventHandle 事件处理接口
type EventHandle interface {
	OnConnect(c *Conn)            // 握手完成之后的回调
	OnData(c *Conn, bytes []byte) // 新消息回调
	OnClose(c *Conn)              // 连接关闭时的回调
}
