package network

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/qigao/ogrenet/gopool"

	"github.com/qigao/ogrenet/avl"
	"github.com/rs/zerolog/log"
)

type OgreNet struct {
	epoll       *OgreEpoll
	connMap     sync.Map
	TimeOutTree *avl.AVLTree
	handle      EventHandle
	limiter     Limiter
}

func (n *OgreNet) Run() {
	n.onTimeOut()
	n.onMessage()
	n.Wait()
}

func NewOgreNet(ip string, port int, opts *Options) *OgreNet {
	ep := NewOgreEpoll(ip, port)
	defaultLimiter := Limiter{
		Timeout: TimeOut{
			conn:   MaxConnTimeout,
			handle: MaxHandleTimeout,
		},
		BufSize: BufSize{
			PacketSize:   MaxPacketSize,
			ReadBufSize:  MaxReadBufSize,
			WriteBufSize: MaxWriteBufSize,
		},
	}
	net := &OgreNet{
		epoll:       ep,
		connMap:     sync.Map{},
		TimeOutTree: avl.NewAvlTree(),
		limiter:     defaultLimiter,
	}
	if opts != nil {
		if opts.TimeOut != (TimeOut{}) {
			net.limiter.Timeout = opts.TimeOut
		}
		if opts.BufSize != (BufSize{}) {
			net.limiter.BufSize = opts.BufSize
		}
		if opts.Packet != (Packet{}) {
			net.limiter.Packet = opts.Packet
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

func (n *OgreNet) onMessage() {
	gopool.Go(func() {
		for dc := range MessageChan {
			log.Info().Msgf("msg rvd:%d,%x", dc.Conn.fd, dc.Msg)
			n.handle.OnMessage(dc.Conn, dc.Msg)
		}
	})
}

func (n *OgreNet) onTimeOut() {
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
	_ = n.TimeOutTree.RemoveNodeValue(c.Updated, c.fd)
	n.connMap.Delete(c.fd)
}

func (n *OgreNet) JoinConn(local *net.TCPConn, remote *net.TCPConn) {
	defer local.Close()
	defer remote.Close()
	_, err := io.Copy(local, remote)
	if err != nil {
		log.Err(err).Msgf("copy failed ", err.Error())
		return
	}
}
