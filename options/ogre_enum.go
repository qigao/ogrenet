package options

type ConnStatus int

const (
	ConnNew     ConnStatus = 1 // 新连接
	ConnClose   ConnStatus = 2 // 关闭连接
	ConnMessage ConnStatus = 3 // 处理消息
)

type PacketType int

const (
	CutByHeadAndTail PacketType = 1 // 头尾分隔
	CutByLength      PacketType = 2 // 长度分隔
	CutByTail        PacketType = 3 // 尾分隔
)

type AlgoType int

const (
	One2One  AlgoType = 1 // 一对一
	HashRing AlgoType = 2 // 一致性哈希
)
