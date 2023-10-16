package network

import (
	"sync"
	"time"

	"github.com/qigao/ogrenet/codecs"

	"github.com/qigao/ogrenet/gopool"

	"github.com/qigao/ogrenet/avl"
	"github.com/rs/zerolog/log"
)

type OgreNet struct {
	epoll       *OgreEpoll
	connMap     sync.Map // 当前的所有连接
	TimeOutTree *avl.AVLTree
	handle      EventHandle
	limiter     Limiter
	BytePool    *sync.Pool //[]byte 的池子
	codec       codecs.Codec
}

func (n *OgreNet) Run() {
	n.checkTimeOut() // 如果过期，就关闭conn
	n.onMessage()    // 如果有新的消息，就走消息处理的逻辑
	n.Wait()
}

func NewOgreNet(ip string, port int, opts *Options) *OgreNet {
	ep := NewOgreEpoll(ip, port)
	defaultLimiter := Limiter{
		Timeout: Timeout{
			conn:   DefaultConnTimeout,
			handle: DefaultHandleTimeout,
		},
	}
	net := &OgreNet{
		epoll:       ep,
		connMap:     sync.Map{},
		TimeOutTree: avl.NewAvlTree(),
		limiter:     defaultLimiter,
	}
	if opts != nil {
		if opts.Limit != (Limiter{}) {
			net.limiter = opts.Limit
		}
		if opts.Codec != nil {
			net.codec = opts.Codec
		}
		if opts.EventHandle != nil {
			net.handle = opts.EventHandle
		}
	}
	return net
}

func (n *OgreNet) Wait() {
	gopool.Go(func() {
		for {
			err := n.epoll.Wait(n.handler)
			if err != nil {
				log.Error().Msgf("epoll wait error: %handle", err)
				continue
			}
		}
	})
}

// 当wait方法取到内容后，会回调此方法，对fd进行处理
func (n *OgreNet) handler(fd int, connType ConnStatus) {
	switch connType {
	case ConnNew:
		nfd, err := n.epoll.Accept(fd)
		if err != nil {
			log.Error().Msgf("Accept error,fd:%d", fd)
			return
		}
		gopool.Go(func() {
			n.onConnected(nfd)
		})
	case ConnMessage:
		c, ok := n.connMap.Load(fd)
		if !ok {
			log.Info().Msgf("Conn fd:%d is not exists", fd)
			n.close(c.(*Conn))
			return
		}
		gopool.Go(func() {
			c.(*Conn).ReadAll()
		})
	default:
		log.Fatal().Msgf("Invalid ConnType: %v", connType)
	}
}

func (n *OgreNet) onConnected(nfd int) {
	msgPool := NewMessagePool()
	conn := NewNetConn(nfd, msgPool)
	conn.SetRemoteAddr(n.epoll.remoteAddr)
	n.connMap.Store(nfd, conn)
	n.TimeOutTree.Add(conn.Updated, nfd)
	n.handle.OnConnect(conn)
}

// 如果有新的消息，就走消息处理的逻辑
func (n *OgreNet) onMessage() {
	gopool.Go(func() {
		for dc := range MessageChan {
			log.Info().Msgf("msg rvd:%d,%x", dc.Conn.fd, dc.Msg)
			n.handle.OnMessage(dc.Conn, dc.Msg)
		}
	})
}

// 判断conn是否已经超时，如果超时就关闭这个conn
func (n *OgreNet) checkTimeOut() {
	gopool.Go(func() {
		for {
			timeOut := time.Now().Unix() - int64(n.limiter.Timeout.conn.Seconds())
			expiredKeys := n.TimeOutTree.GetLessThanKey(timeOut)
			for _, v := range expiredKeys {
				expiredFd := make([]int, 0, 1024)
				expiredFd = append(expiredFd, n.TimeOutTree.Get(v)...)
				for i := 0; i < len(expiredFd); i++ {
					c, ok := n.connMap.Load(expiredFd[i])
					if ok {
						n.close(c.(*Conn))
					}
				}
			}
			time.Sleep(time.Second * 5)
		}
	})
}

// Close
// 系统发送Ctrl+c信号的时候，调用此方法关闭所有的连接
func (n *OgreNet) Close() {
	n.connMap.Range(func(k, v interface{}) bool {
		n.close(v.(*Conn))
		return true
	})
	n.epoll.Close()
}

// CloseFds
// 获取所有的conn 并调用关闭方法
func (n *OgreNet) close(c *Conn) {
	log.Info().Msgf("Deleting Conn fd: %d", c.fd)
	n.epoll.Del(c.fd)
	n.handle.OnClose(c)
	c.Close()
	_ = n.TimeOutTree.RemoveNodeValue(c.Updated, c.fd)
	n.connMap.Delete(c.fd)
}
