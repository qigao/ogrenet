package network

type MsgConn struct {
	Conn *Conn
	Msg  []byte
}

var MessageChan = make(chan *MsgConn, 1024)

var ProxyChan = make(chan *MsgConn, 1024)
