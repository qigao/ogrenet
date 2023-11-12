package ogrenet

import (
	"strconv"
	"sync"
	"time"

	"github.com/qigao/ogrenet/shared/gopool"
	"github.com/qigao/ogrenet/shared/hashring"

	"github.com/qigao/ogrenet/shared/avl"

	"github.com/rs/zerolog/log"
)

func NewOgreNet(ip string, port int, handle EventHandle, options ...OptionFunc) *OgreNet {
	ep := NewOgreEpoll(ip, port)
	ogre := &OgreNet{
		epoll:     ep,
		connMap:   sync.Map{},
		timerTree: avl.NewAvlTree(),
	}
	if handle != nil {
		ogre.handle = handle
	} else {
		log.Fatal().Msgf("EventHandle is nil")
	}
	opts := &Option{}
	limiter := DefaultLimiter()
	opts.Packet = limiter.Packet
	opts.BufSize = limiter.BufSize
	opts.TimeOut = limiter.Timeout
	ogre.msgChan = make(chan *MsgConn, DefaultChanSize)
	for _, ofunc := range options {
		ofunc(opts)
	}

	if opts.LBOption.Load != 0 {
		cfg := hashring.Config{
			PartitionCount:    opts.LBOption.PartitionCount,
			ReplicationFactor: opts.LBOption.ReplicationFactor,
			Load:              opts.LBOption.Load,
			Hasher:            hasher{},
		}
		ogre.hashRing = hashring.New(nil, cfg)
	}
	ogre.opts = opts
	ogre.checkDefaultBufSize()
	return ogre
}

func (n *OgreNet) checkDefaultBufSize() {
	if n.opts.BufSize.PacketSize == 0 {
		n.opts.BufSize.PacketSize = DefaultPacketSize
	}
	if n.opts.BufSize.ReadBufSize == 0 {
		n.opts.BufSize.ReadBufSize = DefaultReadBufSize
	}
	if n.opts.BufSize.WriteBufSize == 0 {
		n.opts.BufSize.WriteBufSize = DefaultWriteBufSize
	}
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
				log.Error().Msgf("epoll wait error: %v handle", err)
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
		n.onConnected(nfd)
	case ConnMessage:
		c, ok := n.connMap.Load(fd)
		if !ok {
			log.Info().Msgf("Conn fd:%d is not exists", fd)
			return
		}
		n.timerTree.Add(c.(*Conn).updated, fd)
		c.(*Conn).ReadAll()
	default:
		log.Debug().Msgf("Invalid ConnType: %v", connType)
	}
}

func (n *OgreNet) onConnected(nfd int) {
	msgPool := NewMessagePool()
	limiter := SetupLimiterOptions(n.opts)
	conn := NewNetConnWithTerm(nfd, msgPool, n.msgChan, &limiter)
	conn.SetRemoteAddr(n.epoll.remoteAddr)
	n.connMap.Store(nfd, conn)
	n.timerTree.Add(conn.updated, nfd)
	n.handle.OnConnect(conn)
	if n.opts.Mode == LoadBalance {
		myConn := ConnMember{
			conn: conn,
		}
		n.hashRing.Add(myConn)
	}
}

func (n *OgreNet) onData() {
	gopool.Go(func() {
		for dc := range n.msgChan {
			conn := dc.Conn
			log.Info().Msgf("pool rvd:%d,%x", conn.fd, dc.Msg)
			n.handle.OnData(conn, dc.Msg)
		}
	})
}

func (n *OgreNet) onTimeOut() {
	gopool.Go(func() {
		for {
			timeCriteria := time.Now().Add(-n.opts.TimeOut.Handle).Unix()
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
	if n.opts.Mode == LoadBalance {
		myConnName := strconv.Itoa(c.fd)
		n.hashRing.Remove(myConnName)
	}
}

// Publish sends the given data to all connected endpoints whose forwarding
// rules match the given pattern. The pattern is compared by simple string
func (n *OgreNet) Publish(data []byte) {
	n.connMap.Range(func(k, v interface{}) bool {
		gopool.Go(func() {
			conn := v.(*Conn)
			conn.Write(data)
			log.Info().Msgf("SendMsgByID fd:%d, len:%d", conn.Fd(), len(data))
		})
		return true
	})
}

// LoadBalance sends the given data to the appropriate connection member based on the hash ring.
func (n *OgreNet) LoadBalance(data []byte) {
	member := n.hashRing.LocateKey(data)
	if member != nil {
		member.(*ConnMember).conn.Write(data)
	}
}

func (n *OgreNet) GetConnByFD(fd int) *Conn {
	c, ok := n.connMap.Load(fd)
	if ok {
		return c.(*Conn)
	}
	return nil
}

func (n *OgreNet) GetAllConns() []*Conn {
	var cons []*Conn
	n.connMap.Range(func(k, v interface{}) bool {
		cons = append(cons, v.(*Conn))
		return true
	})
	return cons
}
