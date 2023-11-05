package ogrenet

import (
	"context"
	"net"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/qigao/ogrenet/shared/avl"

	"github.com/qigao/ogrenet/codecs/passthru"

	"github.com/qigao/ogrenet/shared/bimap"
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
	msgChan   chan *MsgConn
}

type Proxy struct {
	epoll       *OgreEpoll
	connMap     sync.Map
	timerTree   *avl.AVLTree
	limiter     Limiter
	codecPool   *passthru.CodecPool
	handle      ProxyEventHandle
	keepAlive   bool
	proxyMethod ProxyMode
	pattern     string
	hashRing    *consistent.Consistent
	endpoints   *bimap.BiMap[int, string]
	msgChan     chan *MsgConn
}

type MsgConn struct {
	Conn *Conn
	Msg  []byte
}
