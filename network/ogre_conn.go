package network

import (
	"context"
	"net"
	"syscall"
	"time"

	"github.com/qigao/ogrenet/codecs"

	"github.com/rs/zerolog/log"
)

type Conn struct {
	fd         int   // 当前连接的文件描述符 Fd
	Updated    int64 // 最新的更新时间，判断超时用
	ctx        interface{}
	remoteAddr net.Addr
	localAddr  net.Addr
	msg        *MessagePool
	limiter    Limiter
}

func NewNetConn(fd int, msgPool *MessagePool) *Conn {
	conn := &Conn{
		fd:      fd,
		Updated: time.Now().Unix(),
		msg:     msgPool,
		ctx:     context.Background(),
	}

	conn.limiter.Packet.SepType = SepByHeadAndTail
	conn.limiter.Packet.Head = codecs.DefaultMagicHead
	conn.limiter.Packet.Tail = codecs.DefaultMagicTail
	return conn
}

func NewNetConnWithTerm(fd int, msgPool *MessagePool, limiter Limiter) *Conn {
	conn := &Conn{
		fd:      fd,
		Updated: time.Now().Unix(),
		msg:     msgPool,
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

func (c *Conn) terminateMsgByHeadAndTail() {
	readPos := 0
	buf := c.msg.BytePool.Get().([]byte)
	defer func() {
		// buf = make([]byte, DefaultReadBufSize)
		c.msg.BytePool.Put(buf[:MaxReadBufSize])
	}()
	for !c.msg.RBuf.IsEmpty() {
		b, _ := c.msg.RBuf.ReadByte()
		if b == c.limiter.Packet.Head {
			readPos++
		}
		if readPos > 0 {
			buf = append(buf, b)
		}
		if b == c.limiter.Packet.Tail {
			data := buf[MaxReadBufSize-readPos:]
			log.Info().Msgf("Conn fd:%d ReadByHeadTail will process:%x", c.fd, data)
			MessageChan <- &MsgConn{c, data}
			readPos = 0
		}
	}
}

func (c *Conn) terminateMsgByTail() {
	readPos := 0
	buf := c.msg.BytePool.Get().([]byte)
	defer func() {
		// buf = make([]byte, DefaultReadBufSize)
		c.msg.BytePool.Put(buf[:readPos])
	}()
	for !c.msg.RBuf.IsEmpty() {
		b, _ := c.msg.RBuf.ReadByte()
		buf = append(buf, b)
		readPos++
		if b == c.limiter.Packet.Tail {
			data := buf[MaxReadBufSize-readPos:]
			log.Info().Msgf("Conn fd:%d ReadByTail will process:%x", c.fd, data)
			MessageChan <- &MsgConn{c, data}
			readPos = 0
		}
	}
}

func (c *Conn) ReadAll() {
	buf := c.msg.NetRBuf.Get().([]byte)
	defer func() {
		// buf = make([]byte, DefaultReadBufSize)
		c.msg.NetRBuf.Put(buf[:MaxReadBufSize])
	}()
	n, err := c.Read(buf)
	if err != nil {
		log.Error().Msgf("Conn fd: %d read error:%v", c.fd, err)
		c.Close()
		return
	}
	if n > 0 {
		wn, err := c.msg.RBuf.Write(buf[:n])
		if err != nil {
			log.Error().Msgf("Conn fd:%d write to rbuf error:%+v", c.fd, err)
			return
		}
		c.Updated = time.Now().Unix()
		log.Debug().Msgf("Conn fd: %d ReadAll:%x, store len:%d,", c.fd, buf[:n], wn)
		if c.limiter.Packet.Head != 0 && c.limiter.Packet.Tail != 0 && c.limiter.Packet.SepType == SepByHeadAndTail {
			c.terminateMsgByHeadAndTail()
		}
		if c.limiter.Packet.Head == 0 && c.limiter.Packet.Tail != 0 && c.limiter.Packet.SepType == SepByTail {
			c.terminateMsgByTail()
		}
	}
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
	return c.localAddr
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *Conn) SetRemoteAddr(addr net.Addr) {
	c.remoteAddr = addr
}
