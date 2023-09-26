//go:build !windows

package network

import (
	"crypto/tls"
	"errors"
	"github.com/qigao/ogrenet/codecs"
	"github.com/qigao/ogrenet/utils"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alitto/pond"
	"github.com/mailru/easygo/netpoll"
	"github.com/rs/zerolog/log"
)

var (
	poller, _        = netpoll.New(nil)
	bytePool         = utils.NewBytePool(10_000, 65536)
	connId           = int64(0)
	countConnections = int64(0)
	connectionMaps   = map[int64]*Connection{}
	sharedLocker     = sync.Mutex{}
)

type Connection struct {
	conn       net.Conn
	desc       *netpoll.Desc
	worker     *pond.WorkerPool
	codec      *codecs.Message
	options    *Options
	isUdp      bool
	isClosed   bool
	remoteAddr string

	isClient bool
	connId   int64
	mutex    sync.RWMutex
	handler  Handler

	onClose func()
}

func newConnection(rawConn net.Conn, handler Handler, opts *Options, isUdp, isClient bool) *Connection {
	conn := &Connection{
		isUdp:    isUdp,
		isClient: isClient,
		conn:     rawConn,
		options:  opts,
		handler:  handler,
		worker:   pond.New(50, 100),
		mutex:    sync.RWMutex{},
	}

	atomic.AddInt64(&countConnections, 1)
	sharedLocker.Lock()
	connId++
	conn.connId = connId
	connectionMaps[connId] = conn
	sharedLocker.Unlock()
	// 执行启动回调函数
	if !isUdp && conn.handler != nil {
		conn.handler.OnConnect(conn)
	}
	return conn
}

// UDP 建立
func (c *Connection) setupUDP() {
	// 读取数据
	buf := bytePool.Get()
	defer bytePool.Put(buf)
	if c.IsClose() {
		return
	}
	for {
		n, addr, err := c.conn.(*net.UDPConn).ReadFromUDP(buf)
		if err != nil {
			log.Error().Msgf("[CONNECTION] read from error ", err)
			continue
		} else {
			c.remoteAddr = addr.String()
			if c.handler != nil {
				c.handler.OnConnect(c)
			}
		}
		if n > 0 {
			// udp client 不存在接受消息
			if !c.isClient && c.handler != nil {
				if c.options != nil && c.options.EncryptMethod != nil {
					decode, err := c.options.EncryptMethod.Decrypt(buf[:n])
					if err != nil {
						c.fail(errors.New("encryptMethod decrypt bytes fail:" + err.Error()))
					} else {
						c.handler.OnMessage(c, decode)
					}
				} else {
					c.handler.OnMessage(c, buf[:n])
				}
			}
		}
		// Close connection
		if c.handler != nil {
			c.handler.OnClose(c, "")
		}
	}
}

// TCP 建立
func (c *Connection) setupTCP() {
	if c.IsClose() {
		return
	}
	// 设置超时
	if c.options != nil && c.options.Timeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.options.Timeout))
	}
	c.remoteAddr = c.conn.RemoteAddr().String()
	// conn
	desc, err := netpoll.Handle(c.conn, netpoll.EventRead|netpoll.EventEdgeTriggered)
	if err != nil {
		c.fail(err)
		return
	}
	c.desc = desc

	syscallConn, err := c.conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		c.fail(err)
		return
	}

	err = poller.Start(desc, func(ev netpoll.Event) {
		c.worker.Submit(func() {
			// 读取数据
			buf := bytePool.Get()
			for {

				n, err := ReadConn(syscallConn, buf)
				if err != nil && strings.Contains(err.Error(), "timeout") {
					bytePool.Put(buf)
					// 处理读取超时
					_ = c.Close("timeout")
					return
				}
				if n > 0 {
					// 设置超时
					if c.options != nil && c.options.Timeout != 0 {
						c.conn.SetReadDeadline(time.Now().Add(c.options.Timeout))
					}
					if c.codec != nil || c.handler != nil {
						if c.options != nil && c.options.EncryptMethod != nil {
							decode, err := c.options.EncryptMethod.Decrypt(buf[:n])
							if err != nil {
								c.fail(err)
							} else {
								if c.codec != nil {
									c.codec.Write(decode)
								} else {
									c.handler.OnMessage(c, decode)
								}
							}
						} else {
							if c.codec != nil {
								c.codec.Write(buf[:n])
							} else {
								c.handler.OnMessage(c, buf[:n])
							}
						}
					}
				} else {
					break
				}
			}
			bytePool.Put(buf)
			// 处理连接断开事件
			if ev&netpoll.EventReadHup != 0 {
				_ = c.Close("client hup")
				return
			}
		})
	})
	if err != nil {
		c.fail(err)
		return
	}
}

// TLS 建立
func (c *Connection) setupTLS() {
	if c.IsClose() {
		return
	}
	// 设置超时
	if c.options != nil && c.options.Timeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.options.Timeout))
	}

	c.remoteAddr = c.conn.RemoteAddr().String()

	tlsConn, _ := c.conn.(*tls.Conn)
	conn, _ := tlsConn.NetConn().(*net.TCPConn)
	// conn
	desc, err := netpoll.Handle(conn, netpoll.EventRead|netpoll.EventEdgeTriggered)
	if err != nil {
		c.fail(err)
		return
	}
	c.desc = desc
	err = poller.Start(desc, func(ev netpoll.Event) {
		c.worker.Submit(func() {
			// 读取数据
			buf := bytePool.Get()
			// for {
			n, err := tlsConn.Read(buf)
			if err != nil && strings.Contains(err.Error(), "timeout") {
				bytePool.Put(buf)
				// 处理读取超时
				_ = c.Close("timeout")
				return
			}
			if n > 0 {
				// 设置超时
				if c.options != nil && c.options.Timeout != 0 {
					c.conn.SetReadDeadline(time.Now().Add(c.options.Timeout))
				}
				if c.codec != nil || c.handler != nil {
					if c.options != nil && c.options.EncryptMethod != nil {
						decode, err := c.options.EncryptMethod.Decrypt(buf[:n])
						if err != nil {
							c.fail(err)
						} else {
							if c.codec != nil {
								c.codec.Write(decode)
							} else {
								c.handler.OnMessage(c, decode)
							}
						}
					} else {
						if c.codec != nil {
							c.codec.Write(buf[:n])
						} else {
							c.handler.OnMessage(c, buf[:n])
						}
					}
				}
			}
			//	else {
			//		break
			//	}
			//}
			bytePool.Put(buf)
			// 处理连接断开事件
			if ev&netpoll.EventReadHup != 0 {
				_ = c.Close("client hup")
				return
			}
		})
	})
	if err != nil {
		c.fail(err)
		return
	}
}

// 异常
func (c *Connection) fail(err error) {
	log.Error().Err(err)
}

// Close 主动断开连接
func (c *Connection) Close(reason string) error {
	if c.IsClose() {
		return nil
	}
	defer func() {
		if c.worker != nil {
			c.worker.StopAndWait()
		}
	}()
	c.mutex.Lock()
	if !c.isClosed {
		if c.handler != nil {
			c.handler.OnClose(c, reason)
		}
		c.isClosed = true
		atomic.AddInt64(&countConnections, -1)
	}
	c.mutex.Unlock()
	// 执行断开链接回调
	if c.onClose != nil {
		c.onClose()
	}
	// 关闭desc，需要在关闭conn之前
	if c.desc != nil {
		_ = poller.Stop(c.desc)
		_ = c.desc.Close()
	}
	err := c.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

// IsClose 是否已断开
func (c *Connection) IsClose() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.isClosed
}

// Write 下发消息
func (c *Connection) Write(bytes []byte) (n int, err error) {
	if c.IsClose() || c.conn == nil {
		return 0, nil
	}
	if c.options != nil && c.options.EncryptMethod != nil {
		bytes, err := c.options.EncryptMethod.Encrypt(bytes)
		if err != nil {
			c.fail(err)
		}
		return c.conn.Write(bytes)
	} else {
		return c.conn.Write(bytes)
	}
}

// SetBuffer 设置接受消息监听器[注意当设置监听器之后 handler OnMessage将失效]
func (c *Connection) SetBuffer(buffer *codecs.Message) error {
	if c.IsClose() {
		return nil
	}
	// udp client 不存在接受消息 股不存在设置接受消息监听器
	if c.isUdp && c.isClient {
		return errors.New("udp client is not to be set ")
	}
	c.codec = buffer
	return nil
}

// 设置断开连接回调函数
func (c *Connection) SetOnClose(onClose func()) error {
	if c.IsClose() {
		return nil
	}
	c.onClose = onClose
	return nil
}

// RemoteAddr 远端地址
func (c *Connection) RemoteAddr() string {
	if c.remoteAddr == "" && c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return c.remoteAddr
}
