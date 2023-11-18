package ogrenet

import (
	"context"
	"net"
	"sync"

	"github.com/qigao/ogrenet/shared/avl"
	"github.com/qigao/ogrenet/shared/hashring"
)

type Conn struct {
	fd      int
	updated int64
	ctx     context.Context
	rAddr   net.Addr
	lAddr   net.Addr
	pool    *MessagePool
	limiter Limiter
	msgChan chan *MsgConn
}

type OgreNet struct {
	epoll     *OgreEpoll
	connMap   sync.Map
	timerTree *avl.AVLTree
	handle    EventHandle
	opts      *Option
	hashRing  *hashring.Consistent
	msgChan   chan *MsgConn
}

type MsgConn struct {
	Conn *Conn
	Msg  []byte
}
