package ogrenet

import (
	"sync"
	"time"

	"github.com/qigao/ogrenet/shared/avl"

	"github.com/qigao/ogrenet/shared/gopool"

	"github.com/rs/zerolog/log"
)

func (n *OgreNet) Run() {
	n.onTimeOut()
	n.onMessage()
	n.wait()
}

func NewOgreNet(ip string, port int, handle EventHandle, opts *Options) *OgreNet {
	ep := NewOgreEpoll(ip, port)
	defaultLimiter := DefaultLimiter()
	ogre := &OgreNet{
		epoll:     ep,
		connMap:   sync.Map{},
		timerTree: avl.NewAvlTree(),
		limiter:   defaultLimiter,
	}
	if handle != nil {
		ogre.handle = handle
	} else {
		log.Fatal().Msgf("EventHandle is nil")
	}
	ogre.limiter = SetupLimiterOptions(opts)
	return ogre
}

func (n *OgreNet) wait() {
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
		n.timerTree.Add(c.(*Conn).updated, fd)
		gopool.Go(func() {
			c.(*Conn).ReadAll()
		})
	default:
		log.Fatal().Msgf("Invalid ConnType: %v", connType)
	}
}

func (n *OgreNet) onConnected(nfd int) {
	msgPool := NewMessagePool()
	messageChan := make(chan *MsgConn, 1024)
	n.msgChan = messageChan
	conn := NewNetConn(nfd, msgPool, messageChan)
	conn.SetRemoteAddr(n.epoll.remoteAddr)
	n.connMap.Store(nfd, conn)
	n.timerTree.Add(conn.updated, nfd)
	n.handle.OnConnect(conn)
}

func (n *OgreNet) onMessage() {
	gopool.Go(func() {
		for dc := range n.msgChan {
			log.Info().Msgf("rvd fd:%d,msg:%x", dc.Conn.fd, dc.Msg)
			n.handle.OnData(dc.Conn, dc.Msg)
		}
	})
}

func (n *OgreNet) onTimeOut() {
	gopool.Go(func() {
		for {
			timeCriteria := time.Now().Add(-MaxConnTimeout).Unix()
			expiredKeys := n.timerTree.GetLessThanKey(timeCriteria)
			for _, key := range expiredKeys {
				fd := n.timerTree.Get(key)
				for i := 0; i < len(fd); i++ {
					c, ok := n.connMap.Load(fd[i])
					if ok {
						n.close(c.(*Conn))
					} else {
						n.connMap.Delete(fd[i])
						n.timerTree.Remove(key)
					}
				}
			}
			time.Sleep(time.Second * 5)
		}
	})
}

func (n *OgreNet) Close() {
	n.connMap.Range(func(k, v interface{}) bool {
		n.close(v.(*Conn))
		return true
	})
	n.epoll.Close()
}

func (n *OgreNet) close(c *Conn) {
	log.Info().Msgf("Deleting Conn fd: %d", c.fd)
	n.epoll.Del(c.fd)
	n.handle.OnClose(c)
	c.Close()
	n.timerTree.Remove(c.updated)
	n.connMap.Delete(c.fd)
}
