package network

type ConnStatus int

const (
	ConnNew     ConnStatus = 1 // 新连接
	ConnClose   ConnStatus = 2 // 关闭连接
	ConnMessage ConnStatus = 3 // 处理消息
)

type PacketType int

const (
	SepByHeadAndTail PacketType = 1 // 头尾分隔
	SepByLength      PacketType = 2 // 长度分隔
	SepByTail        PacketType = 3 // 尾分隔
)
