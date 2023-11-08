package ogrenet

import (
	"context"
	"net"
	"sync"

	"github.com/qigao/ogrenet/shared/hashring"

	"github.com/qigao/ogrenet/shared/avl"
)

type Conn struct {
	fd      int   // 当前连接的文件描述符 Fd
	updated int64 // 最新的更新时间，判断超时用
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
	limiter   Limiter
	handle    EventHandle
	mode      WorkMode
	hashRing  *hashring.Consistent
	msgChan   chan *MsgConn
}

type MsgConn struct {
	Conn *Conn
	Msg  []byte
}

type (
	PushKey    struct{}
	PubKey     struct{}
	ForwardKey struct{}
	ModeKey    struct{}
)

type PushData struct {
	fd   int
	data []byte
}

type PubData struct {
	data []byte
	fd   []int
}
