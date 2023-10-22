package network

import (
	"bytes"
	"context"
	"net"
	"syscall"
	"time"

	"github.com/qigao/ogrenet/options"

	"github.com/rs/zerolog/log"
)

type Conn struct {
	fd      int   // 当前连接的文件描述符 Fd
	updated int64 // 最新的更新时间，判断超时用
	ctx     interface{}
	rAddr   net.Addr
	lAddr   net.Addr
	pool    *MessagePool
	limiter options.Limiter
	msgChan chan *MsgConn
}

func NewNetConn(fd int, msgPool *MessagePool, msgChan chan *MsgConn) *Conn {
	conn := &Conn{
		fd:      fd,
		updated: time.Now().Unix(),
		pool:    msgPool,
		ctx:     context.Background(),
	}
	conn.msgChan = msgChan
	conn.limiter.Packet.CutType = options.CutByHeadAndTail
	conn.limiter.Packet.Head = options.DefaultMagicHead
	conn.limiter.Packet.Tail = options.DefaultMagicTail
	return conn
}

func NewNetConnWithTerm(fd int, msgPool *MessagePool, limiter options.Limiter) *Conn {
	conn := &Conn{
		fd:      fd,
		updated: time.Now().Unix(),
		pool:    msgPool,
		ctx:     context.Background(),
		limiter: limiter,
	}
	return conn
}

func (c *Conn) Fd() int {
	return c.fd
}

func (c *Conn) Context() interface{} {
	return c.ctx
}

// SetContext sets the context associated with the connection.
func (c *Conn) SetContext(ctx interface{}) {
	c.ctx = ctx
}

func (c *Conn) cutMsgByHeadAndTail() {
	readPos := 0
	buf := c.pool.BytePool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		c.pool.BytePool.Put(buf)
	}()
	for !c.pool.RBuf.IsEmpty() {
		b, _ := c.pool.RBuf.ReadByte()
		if b == c.limiter.Packet.Head {
			buf.WriteByte(b)
			readPos = 1
			continue
		}
		if readPos > 0 {
			buf.WriteByte(b)
			readPos++
		}
		if b == c.limiter.Packet.Tail {
			data := buf.Bytes()
			log.Debug().Msgf("[CutMsgByHeadAndTail] Conn fd:%d  pos:%d msg:%x", c.fd, readPos, data)
			c.msgChan <- &MsgConn{c, data}
			readPos = 0
		}
	}
}

func (c *Conn) cutMsgByTail() {
	readPos := 0
	buf := c.pool.BytePool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		c.pool.BytePool.Put(buf)
	}()
	for !c.pool.RBuf.IsEmpty() {
		b, _ := c.pool.RBuf.ReadByte()
		buf.WriteByte(b)
		readPos++
		if b == c.limiter.Packet.Tail {
			data := buf.Bytes()
			log.Debug().Msgf("[CutMsgByTail] Conn fd:%d  msg:%x", c.fd, data)
			c.msgChan <- &MsgConn{c, data}
			readPos = 0
		}
	}
}

func (c *Conn) ReadAll() {
	buf := make([]byte, options.MaxReadBufSize)
	defer func() {
		buf = nil
	}()
	n, err := c.Read(buf)
	if err != nil {
		log.Error().Msgf("Conn fd: %d read error:%v", c.fd, err)
		c.Close()
		return
	}
	if n > 0 {
		wn, err := c.pool.RBuf.Write(buf[:n])
		if err != nil {
			log.Error().Msgf("Conn fd:%d write to rbuf error:%+v", c.fd, err)
			return
		}
		c.updated = time.Now().Unix()
		log.Debug().Msgf("[ReadAll] Conn fd: %d shared:%x, store len:%d,", c.fd, buf[:n], wn)
		if c.shouldCutByHeadAndTail() {
			c.cutMsgByHeadAndTail()
		}
		if c.shouldCutByTail() {
			c.cutMsgByTail()
		}
	}
}

func (c *Conn) shouldCutByTail() bool {
	headNotAv := c.limiter.Packet.Head == 0
	tailAv := c.limiter.Packet.Tail != 0
	cutByTail := c.limiter.Packet.CutType == options.CutByTail
	return headNotAv && tailAv && cutByTail
}

func (c *Conn) shouldCutByHeadAndTail() bool {
	headAv := c.limiter.Packet.Head != 0
	tailAv := c.limiter.Packet.Tail != 0
	cutByHeadAndTail := c.limiter.Packet.CutType == options.CutByHeadAndTail
	return headAv && tailAv && cutByHeadAndTail
}

func (c *Conn) Read(b []byte) (n int, err error) {
	n, err = syscall.Read(c.fd, b)
	return
}

// Write writes a message to the connection.
func (c *Conn) Write(message []byte) (int, error) {
	n, err := syscall.Write(c.fd, message)
	if err != nil {
		log.Error().Msgf("Conn fd:%d Write error:%+v", c.fd, err)
	}
	c.updated = time.Now().Unix()
	return n, err
}

func (c *Conn) Close() error {
	return syscall.Close(c.fd)
}

func (c *Conn) SetDeadline(t time.Time) error {
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (c *Conn) LocalAddr() net.Addr {
	return c.lAddr
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.rAddr
}

func (c *Conn) SetRemoteAddr(addr net.Addr) {
	c.rAddr = addr
}
