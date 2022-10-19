package network

import (
	"sync"

	"github.com/qigao/ogrenet/codecs/passthru"

	"github.com/qigao/ogrenet/shared/avl"
	"github.com/qigao/ogrenet/shared/bimap"

	"github.com/qigao/ogrenet/options"
)

type OgreNet struct {
	epoll     *OgreEpoll
	connMap   sync.Map
	timerTree *avl.AVLTree
	limiter   options.Limiter
	handle    EventHandle
}

type OgreNetProxy struct {
	epoll     *OgreEpoll
	connMap   sync.Map
	timerTree *avl.AVLTree
	limiter   options.Limiter
	codecPool *passthru.CodecPool
	handle    ProxyEventHandle
	keepAlive bool
	proxyAlgo options.AlgoType
	endpoints *bimap.BiMap[int, string]
}
