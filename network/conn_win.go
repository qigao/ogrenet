//go:build windows

// windows 无epoll  故需要区分

package network

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qigao/ogrenet/codecs"
	"github.com/qigao/ogrenet/utils"

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
	locker   sync.RWMutex
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
		locker:   sync.RWMutex{},
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
			log.Error("[CONNECTION] read from error ", err)
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
	// 读取数据
	buf := bytePool.Get()
	for {
		n, err := c.conn.Read(buf)
		if n > 0 {
			// 设置超时
			if c.options != nil && c.options.Timeout != 0 {
				c.conn.SetReadDeadline(time.Now().Add(c.options.Timeout))
			}
			if c.buffer != nil || c.handler != nil {
				if c.options != nil && c.options.EncryptMethod != nil {
					decode, err := c.options.EncryptMethod.Decrypt(buf[:n])
					if err != nil {
						c.fail(err)
					} else {
						if c.buffer != nil {
							c.buffer.Write(decode)
						} else {
							c.handler.OnMessage(c, decode)
						}
					}
				} else {
					if c.buffer != nil {
						c.buffer.Write(buf[:n])
					} else {
						c.handler.OnMessage(c, buf[:n])
					}
				}
			}
		}
		if err != nil {
			bytePool.Put(buf)
			if io.EOF == err {
				// 连接断开
				_ = c.Close("client close")
				return
			}
			if strings.Contains(err.Error(), "timeout") {
				// 读取超时
				_ = c.Close("timeout")
				return
			}
		}
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
	// 读取数据
	buf := bytePool.Get()
	for {
		n, err := c.conn.Read(buf)
		if n > 0 {
			// 设置超时
			if c.options != nil && c.options.Timeout != 0 {
				c.conn.SetReadDeadline(time.Now().Add(c.options.Timeout))
			}
			if c.buffer != nil || c.handler != nil {
				if c.options != nil && c.options.EncryptMethod != nil {
					decode, err := c.options.EncryptMethod.Decrypt(buf[:n])
					if err != nil {
						c.fail(err)
					} else {
						if c.buffer != nil {
							c.buffer.Write(decode)
						} else {
							c.handler.OnMessage(c, decode)
						}
					}
				} else {
					if c.buffer != nil {
						c.buffer.Write(buf[:n])
					} else {
						c.handler.OnMessage(c, buf[:n])
					}
				}
			}
		}
		if err != nil {
			bytePool.Put(buf)
			if io.EOF == err {
				// 连接断开
				_ = c.Close("client close")
				return
			}
			if strings.Contains(err.Error(), "timeout") {
				// 读取超时
				_ = c.Close("timeout")
				return
			}
		}
	}
}

// 异常
func (c *Connection) fail(err error) {
	log.Fatal(err)
}

// Close 主动断开连接
func (c *Connection) Close(reason string) error {
	if c.IsClose() {
		return nil
	}
	defer func() {
		c.worker.Wait()
	}()

	c.locker.Lock()
	if !c.isClosed {
		if c.handler != nil {
			c.handler.OnClose(c, reason)
		}
		c.isClosed = true
		atomic.AddInt64(&countConnections, -1)
	}
	c.locker.Unlock()
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
	c.locker.RLock()
	defer c.locker.RUnlock()
	return c.isClosed
}

// Write 下发消息
func (c *Connection) Write(bytes []byte) (n int, err error) {
	if c.IsClose() {
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
	c.buffer = buffer
	return nil
}

// 设置断开连接回调函数
func (c *Connection) SetOnClose(onClose func()) error {
	if c.IsClose() {
		return errors.New("the connection is close")
	}
	c.onClose = onClose
	return nil
}

// RemoteAddr 远端地址
func (c *Connection) RemoteAddr() string {
	if c.remoteAddr == "" && !c.IsClose() {
		return c.conn.RemoteAddr().String()
	}
	return c.remoteAddr
}
