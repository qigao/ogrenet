package ogrenet

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/qigao/ogrenet/shared/gopool"

	"github.com/qigao/ogrenet/codecs/passthru"
	"github.com/qigao/ogrenet/shared/avl"
	"github.com/qigao/ogrenet/shared/bimap"

	"github.com/rs/zerolog/log"
)

func NewOgreNetProxy(ip string, port int, handle ProxyEventHandle, opts *Options) *Proxy {
	ep := NewOgreEpoll(ip, port)
	defaultLimiter := DefaultLimiter()
	ogre := &Proxy{
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
	if opts.proxy.mode != ProxyNone {
		ogre.proxyMethod = opts.proxy.mode
		ogre.pattern = opts.proxy.pattern
	}
	if opts.rotateCfg.Load != 0 {
		cfg := consistent.Config{
			PartitionCount:    opts.rotateCfg.PartitionCount,
			ReplicationFactor: opts.rotateCfg.ReplicationFactor,
			Load:              opts.rotateCfg.Load,
			Hasher:            hasher{},
		}
		ogre.hashRing = consistent.New(nil, cfg)
	}
	ogre.codecPool = passthru.NewCodecPool()
	ogre.endpoints = bimap.NewBiMap[int, string]()
	return ogre
}

func (p *Proxy) Run() {
	p.onTimeOut()
	p.onData()
	p.wait()
}

func (p *Proxy) wait() {
	gopool.Go(func() {
		for {
			err := p.epoll.Wait(p.handler)
			if err != nil {
				log.Error().Msgf("epoll wait error: %handle", err)
				continue
			}
		}
	})
}

func (p *Proxy) handler(fd int, connType ConnStatus) {
	switch connType {
	case ConnNew:
		nfd, err := p.epoll.Accept(fd)
		if err != nil {
			log.Error().Msgf("Accept error,fd:%d", fd)
			return
		}
		gopool.Go(func() {
			p.onConnected(nfd)
		})
	case ConnMessage:
		c, ok := p.connMap.Load(fd)
		if !ok {
			log.Info().Msgf("Conn fd:%d is not exists", fd)
			p.close(c.(*Conn))
			return
		}
		p.timerTree.Add(c.(*Conn).updated, fd)
		gopool.Go(func() {
			c.(*Conn).ReadAll()
		})
	default:
		log.Fatal().Msgf("Invalid ConnType: %v", connType)
	}
}

func (p *Proxy) onConnected(nfd int) {
	msgPool := NewMessagePool()
	proxyChan := make(chan *MsgConn, 1024)
	p.msgChan = proxyChan
	conn := NewNetConn(nfd, msgPool, proxyChan)
	conn.SetRemoteAddr(p.epoll.remoteAddr)
	p.connMap.Store(nfd, conn)
	p.timerTree.Add(conn.updated, nfd)

	p.handle.OnConnect(conn)
	if p.proxyMethod == Rotate {
		myConn := ConnMember{
			conn: conn,
		}
		p.hashRing.Add(myConn)
	}
}

func (p *Proxy) onData() {
	gopool.Go(func() {
		for dc := range p.msgChan {
			log.Info().Msgf("pool rvd:%d,%x", dc.Conn.fd, dc.Msg)
			codec := p.codecPool.NewEmptyPassThruCodecFromPool()
			codec.Decode(dc.Msg)
			switch codec.Head.CodecType {
			case passthru.Register:
				p.timerTree.Add(dc.Conn.updated, dc.Conn.fd)
				p.endpoints.Insert(dc.Conn.fd, string(codec.Head.ID[:]))
				ack := passthru.NewAckCodec(codec.Head.ID, RegisterCMD)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
				p.handle.OnRegister(dc.Conn)
			case passthru.UnRegister:
				p.endpoints.Delete(dc.Conn.fd)
				ack := passthru.NewAckCodec(codec.Head.ID, UnregisterCMD)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
				p.handle.OnUnRegister(dc.Conn)
			case passthru.HeartBeat:
				p.timerTree.Add(dc.Conn.updated, dc.Conn.fd)
				ack := passthru.NewAckCodec(codec.Head.ID, HeartbeatCMD)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
			case passthru.Data:
				p.timerTree.Add(dc.Conn.updated, dc.Conn.fd)
				ack := passthru.NewAckCodec(codec.Head.ID, DataCMD)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
				gopool.Go(func() {
					p.handle.OnData(dc.Conn, codec.GetBody())
					p.ForwardData(dc.Conn, p.proxyMethod, codec.GetBody())
				})
			case passthru.Close:
				p.endpoints.Delete(dc.Conn.fd)
				ack := passthru.NewAckCodec(codec.Head.ID, CloseCMD)
				ackBytes, _ := ack.Encode()
				dc.Conn.Write(ackBytes)
				p.handle.OnClose(dc.Conn)
			// case passthru.Error:
			// 	p.handle.OnError(dc.Conn)
			// case ReConnect:
			// 	p.handle.OnReConnect(dc.Conn)
			default:
				log.Fatal().Msgf("Invalid CodecType: %v", codec.Head.CodecType)
			}

		}
	})
}

func (p *Proxy) onTimeOut() {
	gopool.Go(func() {
		for {
			timeCriteria := time.Now().Add(-MaxConnTimeout).Unix()
			expiredKeys := p.timerTree.GetLessThanKey(timeCriteria)
			for _, key := range expiredKeys {
				fd := p.timerTree.Get(key)
				for i := 0; i < len(fd); i++ {
					c, ok := p.connMap.Load(fd[i])
					if ok {
						p.close(c.(*Conn))
					} else {
						p.connMap.Delete(fd[i])
						p.timerTree.Remove(key)
					}
				}
			}
			time.Sleep(time.Second * 5)
		}
	})
}

func (p *Proxy) Close() {
	p.connMap.Range(func(k, v interface{}) bool {
		p.close(v.(*Conn))
		return true
	})
	p.epoll.Close()
}

func (p *Proxy) close(c *Conn) {
	log.Info().Msgf("Closing Conn fd: %d", c.fd)
	p.handle.OnClose(c)
	p.epoll.Del(c.fd)
	c.Close()
	log.Debug().Msgf("Removing fd: %d and Conn", c.fd)
	p.timerTree.Remove(c.updated)
	p.connMap.Delete(c.fd)
	p.endpoints.Delete(c.fd)
	if p.proxyMethod == Rotate {
		myConnName := strconv.Itoa(c.fd)
		p.hashRing.Remove(myConnName)
	}
}

// Push sends the given data to the endpoint with the specified ID.
// If the endpoint is not found, the data is not sent.
func (p *Proxy) Push(dstId string, data []byte) {
	val, ok := p.endpoints.GetInverse(dstId)
	if ok {
		conn, found := p.connMap.Load(val)
		if found {
			gopool.Go(func() {
				n, err := conn.(*Conn).Write(data)
				log.Debug().Msgf("SendMsgByID fd:%d, len:%d, err:%v", conn.(*Conn).fd, n, err)
			})
		}
	}
}

// Publish sends the given data to all connected endpoints whose forwarding
// rules match the given pattern. The pattern is compared by simple string
func (p *Proxy) Publish(pattern string, data []byte) {
	for k, v := range p.endpoints.GetForwardMap() {
		if strings.Contains(v, pattern) {
			conn, found := p.connMap.Load(k)
			if found {
				gopool.Go(func() {
					n, err := conn.(*Conn).Write(data)
					log.Debug().Msgf("SendMsgByPattern fd:%d, len:%d, err:%v", conn.(*Conn).fd, n, err)
				})
			}
		}
	}
}

// Router sends the given data to all connected clients through their respective connections.
// It uses a goroutine pool to write the data asynchronously to each connection.
func (p *Proxy) Router(data []byte) {
	p.connMap.Range(func(k, v interface{}) bool {
		gopool.Go(func() {
			n, err := v.(*Conn).Write(data)
			log.Debug().Msgf("SendMsgToAll fd:%d, len:%d, err:%v", v.(*Conn).fd, n, err)
		})
		return true
	})
}

func (p *Proxy) ForwardData(conn *Conn, method ProxyMode, data []byte) {
	switch method {
	case Push:
		p.Push(p.pattern, data)
	case Publish:
		p.Publish(p.pattern, data)
	case Rotate:
		gopool.Go(func() {
			member := p.hashRing.LocateKey(data)
			if member != nil {
				member.(*ConnMember).conn.Write(data)
			}
		})
	default:
		log.Error().Msgf("Invalid Forward Type: %v", method)
	}
}
