package ogrenet

import (
	"strconv"
	"sync"
	"time"

	"github.com/qigao/ogrenet/shared/hashring"

	"github.com/qigao/ogrenet/shared/gopool"

	"github.com/qigao/ogrenet/shared/avl"

	"github.com/rs/zerolog/log"
)

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
	limiter := SetupLimiterOptions(opts)
	ogre.limiter = limiter

	if opts.rotateCfg.Load != 0 {
		cfg := hashring.Config{
			PartitionCount:    opts.rotateCfg.PartitionCount,
			ReplicationFactor: opts.rotateCfg.ReplicationFactor,
			Load:              opts.rotateCfg.Load,
			Hasher:            hasher{},
		}
		ogre.hashRing = hashring.New(nil, cfg)
	}
	return ogre
}

func (n *OgreNet) Run() {
	n.onTimeOut()
	n.wait()
	n.onData()
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
	proxyChan := make(chan *MsgConn, 1024)
	n.msgChan = proxyChan
	conn := NewNetConn(nfd, msgPool, proxyChan)
	conn.SetRemoteAddr(n.epoll.remoteAddr)
	n.connMap.Store(nfd, conn)
	n.timerTree.Add(conn.updated, nfd)

	n.handle.OnConnect(conn)
	n.mode = conn.getMode()
	if n.mode == Rotate {
		myConn := ConnMember{
			conn: conn,
		}
		n.hashRing.Add(myConn)
	}
}

func (n *OgreNet) onData() {
	gopool.Go(func() {
		for dc := range n.msgChan {
			log.Info().Msgf("pool rvd:%d,%x", dc.Conn.fd, dc.Msg)
			n.handle.OnData(dc.Conn, dc.Msg)
			switch n.mode {
			case Push:
				data := dc.Conn.getPushData()
				n.push(data.fd, data.data)
			case Publish:
				data := dc.Conn.getPubData()
				n.publish(data.fd, data.data)
			case Rotate:
				data := dc.Conn.getForwardData()
				n.forwardData(data)
			default:
				log.Fatal().Msgf("Invalid ProxyMode: %v", n.mode)
			}
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
	log.Info().Msgf("Closing Conn fd: %d", c.fd)
	n.handle.OnClose(c)
	n.epoll.Del(c.fd)
	c.Close()
	log.Debug().Msgf("Removing fd: %d and Conn", c.fd)
	n.timerTree.Remove(c.updated)
	n.connMap.Delete(c.fd)
	if n.mode == Rotate {
		myConnName := strconv.Itoa(c.fd)
		n.hashRing.Remove(myConnName)
	}
}

// push sends the given data to the endpoint with the specified ID.
// If the endpoint is not found, the data is not sent.
func (n *OgreNet) push(nfd int, data []byte) {
	conn, found := n.connMap.Load(nfd)
	if found {
		gopool.Go(func() {
			n, err := conn.(*Conn).Write(data)
			log.Debug().Msgf("SendMsgByID fd:%d, len:%d, err:%v", conn.(*Conn).fd, n, err)
		})
	}
}

// publish sends the given data to all connected endpoints whose forwarding
// rules match the given pattern. The pattern is compared by simple string
func (n *OgreNet) publish(nfd []int, data []byte) {
	for _, fd := range nfd {
		conn, found := n.connMap.Load(fd)
		if found {
			gopool.Go(func() {
				n, err := conn.(*Conn).Write(data)
				log.Debug().Msgf("SendMsgByPattern fd:%d, len:%d, err:%v", conn.(*Conn).fd, n, err)
			})
		}
	}
}

func (n *OgreNet) forwardData(data []byte) {
	gopool.Go(func() {
		member := n.hashRing.LocateKey(data)
		if member != nil {
			member.(*ConnMember).conn.Write(data)
		}
	})
}
