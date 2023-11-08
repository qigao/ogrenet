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

type WorkMode int

const (
	UnknowMode WorkMode = iota
	ServerMode
	PushMode   // 一对一
	PubMode    // 一对多
	RotateMode // 轮询
)

func IsProxyModeValid(value WorkMode) bool {
	return value >= PushMode && value <= RotateMode
}
