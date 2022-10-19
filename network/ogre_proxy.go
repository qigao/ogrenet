package network

import (
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
	ogre.codecPool = passthru.NewCodecPool()
	ogre.endpoints = bimap.NewBiMap[int, string]()
	return ogre
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
			codec := n.codecPool.Passthru.Get().(*passthru.CodecPassThru)
			codec.Decode(dc.Msg)
			switch codec.Head.CodecType {
			case passthru.Register:
				n.timerTree.Add(dc.Conn.updated, dc.Conn.fd)
				n.endpoints.Insert(dc.Conn.fd, string(codec.Head.ID[:]))
				n.handle.OnRegister(dc.Conn)
				ack := passthru.NewAckCodec(codec.Head.ID, Register)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
			case passthru.UnRegister:
				n.endpoints.Delete(dc.Conn.fd)
				n.handle.OnUnRegister(dc.Conn)
				ack := passthru.NewAckCodec(codec.Head.ID, Unregister)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
			case passthru.HeartBeat:
				n.timerTree.Add(dc.Conn.updated, dc.Conn.fd)
				ack := passthru.NewAckCodec(codec.Head.ID, Heartbeat)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
			case passthru.Data:
				n.timerTree.Add(dc.Conn.updated, dc.Conn.fd)
				n.handle.OnData(dc.Conn, codec.GetBody())
				ack := passthru.NewAckCodec(codec.Head.ID, Data)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
			case passthru.Close:
				n.endpoints.Delete(dc.Conn.fd)
				n.handle.OnClose(dc.Conn)
				ack := passthru.NewAckCodec(codec.Head.ID, Close)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
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

func (n *OgreNetProxy) FindConnByID(dstId string) *Conn {
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
