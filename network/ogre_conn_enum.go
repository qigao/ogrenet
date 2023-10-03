package network

type ConnStatus int

const (
	ConnNew     ConnStatus = 1 // 新连接
	ConnClose   ConnStatus = 2 // 关闭连接
	ConnMessage ConnStatus = 3 // 处理消息
)
