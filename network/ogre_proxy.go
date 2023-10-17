package network

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/qigao/ogrenet/shared/gopool"

	"github.com/qigao/ogrenet/codecs/passthru"
	"github.com/qigao/ogrenet/shared/avl"
	"github.com/qigao/ogrenet/shared/bimap"

	"github.com/qigao/ogrenet/options"
	"github.com/rs/zerolog/log"
)

func NewOgreNetProxy(ip string, port int, handle ProxyEventHandle, opts *options.Options) *OgreNetProxy {
	ep := NewOgreEpoll(ip, port)
	defaultLimiter := options.DefaultLimiter()
	ogre := &OgreNetProxy{
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
	options.SetupLimiterOptions(opts)
	ogre.codecPool = NewCodecPool()
	ogre.endpoints = bimap.NewBiMap[int, string]()
	return ogre
}

func (n *OgreNetProxy) JoinConn(local *net.TCPConn, remote *net.TCPConn) {
	defer local.Close()
	defer remote.Close()
	_, err := io.Copy(local, remote)
	if err != nil {
		log.Err(err).Msg("copy failed ")
		return
	}
}

func (n *OgreNetProxy) FindBackendConn(dstId string) *Conn {
	val, ok := n.endpoints.GetInverse(dstId)
	if ok {
		conn, found := n.connMap.Load(val)
		if found {
			return conn.(*Conn)
		}
		return nil
	}
	return nil
}

func (n *OgreNetProxy) Register(dstId string, conn *Conn) {
	n.endpoints.Insert(conn.fd, dstId)
}

func (n *OgreNetProxy) UnRegister(dstId string, conn *Conn) {
	n.endpoints.Delete(conn.fd)
}

func (n *OgreNetProxy) HeartBeat(dstId string, conn *Conn) {
	// conn.Write( /*heartbeat*/ )
}

func (n *OgreNetProxy) Run() {
	n.onTimeOut()
	n.onData()
	n.wait()
}

func (n *OgreNetProxy) wait() {
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

func (n *OgreNetProxy) handler(fd int, connType options.ConnStatus) {
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

func (n *OgreNetProxy) onConnected(nfd int) {
	msgPool := NewMessagePool()
	conn := NewNetConn(nfd, msgPool, ProxyChan)
	conn.SetRemoteAddr(n.epoll.remoteAddr)
	n.connMap.Store(nfd, conn)
	n.timerTree.Add(conn.updated, nfd)
	n.handle.OnConnect(conn)
}

func (n *OgreNetProxy) onData() {
	gopool.Go(func() {
		for dc := range ProxyChan {
			log.Info().Msgf("msg rvd:%d,%x", dc.Conn.fd, dc.Msg)
			codec := n.codecPool.passthru.Get().(*passthru.CodecPassThru)
			codec.Decode(dc.Msg)
			switch codec.Head.CodecType {
			case passthru.Register:
				n.handle.OnRegister(dc.Conn)
			case passthru.Unregister:
				n.handle.OnUnRegister(dc.Conn)
			case passthru.HeartBeat:
				n.handle.OnHeartBeat(dc.Conn)
			case passthru.Data:
				n.handle.OnData(dc.Conn, codec.GetBody())
			case passthru.Ack:
				n.handle.OnAck(dc.Conn)
			case passthru.Close:
				n.handle.OnClose(dc.Conn)
			// case passthru.Error:
			// 	n.handle.OnError(dc.Conn)
			// case passthru.ReConnect:
			// 	n.handle.OnReConnect(dc.Conn)
			default:
				log.Fatal().Msgf("Invalid CodecType: %v", codec.Head.CodecType)
			}
		}
	})
}

func (n *OgreNetProxy) onTimeOut() {
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

func (n *OgreNetProxy) Close() {
	n.connMap.Range(func(k, v interface{}) bool {
		n.close(v.(*Conn))
		return true
	})
	n.epoll.Close()
}

func (n *OgreNetProxy) close(c *Conn) {
	log.Info().Msgf("Deleting Conn fd: %d", c.fd)
	n.epoll.Del(c.fd)
	n.handle.OnClose(c)
	c.Close()
	_ = n.timerTree.RemoveNodeValue(c.updated, c.fd)
	n.connMap.Delete(c.fd)
	n.endpoints.Delete(c.fd)
}
