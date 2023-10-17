package network

type Options struct {
	TimeOut       TimeOut
	Packet        Packet
	BufSize       BufSize
	CompressLevel int
	numPoller     int
	EventHandle   EventHandle
}
