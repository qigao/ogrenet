package ogrenet

type ConnStatus int

const (
	ConnNew ConnStatus = iota
	ConnClose
	ConnMessage
)

type PacketType int

const (
	SepByHeadAndTail PacketType = iota
	SepByLength
	SepByTail
)

type WorkMode int

const (
	UnknowMode WorkMode = iota
	ServerMode
	PubMode
	LoadBalance
)

func IsValidWorkMode(value WorkMode) bool {
	return value >= ServerMode && value <= LoadBalance
}
