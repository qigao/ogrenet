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
	ep          *OgreEpoll
	conns       sync.Map // 当前的所有连接
	TimeOutTree *avl.AVLTree
	handle      EventHandle
	timeOut     int64
	BytePool    *sync.Pool //[]byte 的池子
	codec       codecs.Codec
}

func (n *OgreNet) Run() {
	n.checkTimeOut() // 如果过期，就关闭conn
	// n.checkMessage() // 如果有消息，就调用 conn.read方法解包
	n.getMessage() // 如果有新的消息，就走消息处理的逻辑
	// n.Push()
	// n.closeConn()
	n.EpollWait()
}

func NewOgreNet(ip string, port int, handle EventHandle, opts *Options) *OgreNet {
	ep := NewOgreEpoll(ip, port)
	net := &OgreNet{
		ep:          ep,
		handle:      handle,
		conns:       sync.Map{},
		TimeOutTree: avl.NewAvlTree(),
		timeOut:     30,
	}
	if opts != nil {
		// if opts.ReadBufferSize > 0 {
		// 	netaddr.readBufferSize = opts.ReadBufferSize
		// }
		// if opts.WriteBufferSize > 0 {
		// 	netaddr.WriteBufferSize = opts.WriteBufferSize
		// }
		if opts.ConnectionTimeOut > 0 {
			net.timeOut = opts.ConnectionTimeOut
		}
		if opts.Codec != nil {
			net.codec = opts.Codec
		}
	}

	return net
}

func (n *OgreNet) EpollWait() {
	for {
		err := n.ep.Wait(n.handler)
		if err != nil {
			log.Error().Msgf("epoll wait error: %handle", err)
			continue
		}
	}
}

// 当wait方法取到内容后，会回调此方法，对fd进行处理
func (n *OgreNet) handler(fd int, connType ConnStatus) {
	switch connType {
	case ConnNew:
		nfd, err := n.ep.Accept(fd)
		if err != nil {
			log.Error().Msgf("accept error,fd is %d", fd)
			return
		}
		n.onConnected(nfd)
	case ConnMessage:
		log.Info().Msgf("接收到描述符为%v的消息", fd)
		c, ok := n.conns.Load(fd)
		if !ok {
			log.Info().Msgf("描述符fd 为 %d 的s.conns 不存在！", fd)
			return
		}
		c.(*Conn).ReadToMemory()
	default:
		panic("no connType")
	}
}

func (n *OgreNet) onConnected(nfd int) {
	msgPool := NewMessagePool()
	conn := NewConn(nfd, msgPool)
	conn.SetRemoteAddr(n.ep.remoteAddr)
	n.conns.Store(nfd, conn)
	n.TimeOutTree.Add(conn.UpdateTime, nfd)
	n.handle.OnConnect(conn)
}

// 如果有新的消息，就走消息处理的逻辑
func (n *OgreNet) getMessage() {
	go func() {
		for dc := range MessageChan {
			log.Info().Msgf("接收到消息:%x", dc.Msg)
			n.handle.OnMessage(dc.Conn, dc.Msg)
		}
	}()
}

// 判断conn是否已经超时，如果超时就关闭这个conn
func (n *OgreNet) checkTimeOut() {
	gopool.Go(func() {
		for {
			//handle.conns.Range(func(k, v interface{}) bool {
			//	if time.Now().Sub(v.(*Conn).updateTime) >= time.Second* handle.connTimeout {
			//		log.Info().Msgf("fd 为 %d 的连接即将被断开\n", v.(*Conn).fd)
			//		handle.closeFd(v.(*Conn))
			//	}
			//	return true
			//})
			//time.Sleep(time.Second * 2)
			/**********************************更改删除超时连接的结构为平衡二叉树*************************************/
			//给定一个值，获取小于该值的所有元素
			timeOutint64 := time.Now().Unix() - n.timeOut
			// fmt.Println("当前时间：", time.Now().Unix())
			expiredKeys := n.TimeOutTree.GetLessThanKey(timeOutint64)
			// 删除conns中的已超时的fd
			for _, v := range expiredKeys {
				slice := make([]int, 0, 1024)
				slice = append(slice, n.TimeOutTree.Get(v)...)
				for i := 0; i < len(slice); i++ {
					c, _ := n.conns.Load(slice[i])
					n.closeFd(c.(*Conn))
				}

			}
			// 删除树中已超时的fd
			// handle.checkTimeOutTree.RemoveOneNodeAndChilds(node.Key)

			// fmt.Println(handle.checkTimeOutTree.InOrder(-1))

			time.Sleep(time.Second)
			/**********************************更改删除超时连接的结构为平衡二叉树END**********************************/
		}
	})
}

// Close
// 系统发送Ctrl+c信号的时候，调用此方法关闭所有的连接
func (n *OgreNet) Close() {
	n.CloseFds()
	n.ep.Close()
}

// CloseFds
// 获取所有的conn 并调用关闭方法
func (n *OgreNet) CloseFds() {
	n.conns.Range(func(k, v interface{}) bool {
		n.handle.OnClose(v.(*Conn))
		n.closeFd(v.(*Conn))
		return true
	})
}

func (n *OgreNet) closeFd(c *Conn) {
	n.ep.Del(c.fd)
	c.Close()
	log.Info().Msgf("正在删除fd=%d的连接", c.fd)
	_ = n.TimeOutTree.RemoveNodeValue(c.UpdateTime, c.fd)
	n.conns.Delete(c.fd)
	n.handle.OnClose(c)
}
