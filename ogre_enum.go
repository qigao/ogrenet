package ogrenet

type ConnStatus int

const (
	ConnNew     ConnStatus = iota // 新连接
	ConnClose                     // 关闭连接
	ConnMessage                   // 处理消息
)

type PacketType int

const (
	CutByHeadAndTail PacketType = iota // 头尾分隔
	CutByLength                        // 长度分隔
	CutByTail                          // 尾分隔
)

type ProxyMode int

const (
	ProxyNone ProxyMode = iota
	Push                // 一对一
	Publish             // 一对多
	Rotate              // 轮询
)

func IsProxyModeValid(value ProxyMode) bool {
	return value >= Push && value <= Rotate
}
