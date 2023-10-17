package network

import (
	"sync"
	"time"

	"github.com/qigao/ogrenet/shared/gopool"

	"github.com/qigao/ogrenet/shared/avl"

	"github.com/qigao/ogrenet/options"

	"github.com/rs/zerolog/log"
)

func (n *OgreNet) Run() {
	n.onTimeOut()
	n.onMessage()
	n.wait()
}

func NewOgreNet(ip string, port int, handle EventHandle, opts *options.Options) *OgreNet {
	ep := NewOgreEpoll(ip, port)
	defaultLimiter := options.DefaultLimiter()
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
	ogre.limiter = options.SetupLimiterOptions(opts)
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

func (n *OgreNet) handler(fd int, connType options.ConnStatus) {
	switch connType {
	case options.ConnNew:
		nfd, err := n.epoll.Accept(fd)
		if err != nil {
			log.Error().Msgf("Accept error,fd:%d", fd)
			return
		}
		gopool.Go(func() {
			n.onConnected(nfd)
		})
	case options.ConnMessage:
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
	conn := NewNetConn(nfd, msgPool, MessageChan)
	conn.SetRemoteAddr(n.epoll.remoteAddr)
	n.connMap.Store(nfd, conn)
	n.timerTree.Add(conn.updated, nfd)
	n.handle.OnConnect(conn)
}

func (n *OgreNet) onMessage() {
	gopool.Go(func() {
		for dc := range MessageChan {
			log.Info().Msgf("msg rvd:%d,%x", dc.Conn.fd, dc.Msg)
			n.handle.OnData(dc.Conn, dc.Msg)
		}
	})
}

func (n *OgreNet) onTimeOut() {
	gopool.Go(func() {
		for {
			timeOut := time.Now().Unix() - int64(n.limiter.Timeout.Conn.Seconds())
			expiredKeys := n.timerTree.GetLessThanKey(timeOut)
			for _, v := range expiredKeys {
				expiredFd := make([]int, 0, 1024)
				expiredFd = append(expiredFd, n.timerTree.Get(v)...)
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
	_ = n.timerTree.RemoveNodeValue(c.updated, c.fd)
	n.connMap.Delete(c.fd)
}
