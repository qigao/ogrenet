package network

type MsgConn struct {
	Conn *Conn
	Msg  []byte
}

var (
	OpenedConn  = make(chan *Conn, 1024)
	ClosedConn  = make(chan *Conn, 1024)
	MessageChan = make(chan *MsgConn, 1024)
)
