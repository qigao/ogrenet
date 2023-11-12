package ogrenet

import (
	"bytes"
	"context"
	"net"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

func NewNetConn(fd int, msgPool *MessagePool, msgChan chan *MsgConn) *Conn {
	conn := &Conn{
		fd:      fd,
		updated: time.Now().Unix(),
		pool:    msgPool,
		ctx:     context.Background(),
	}
	conn.msgChan = msgChan
	conn.limiter = DefaultLimiter()
	return conn
}

func NewNetConnWithTerm(fd int, msgPool *MessagePool, msgChan chan *MsgConn, limiter *Limiter) *Conn {
	conn := NewNetConn(fd, msgPool, msgChan)
	emptyLimiter := Limiter{}
	if emptyLimiter == *limiter {
		return conn
	}
	conn.limiter = *limiter
	return conn
}

func (c *Conn) Fd() int {
	return c.fd
}

func (c *Conn) Context() context.Context {
	return c.ctx
}

// SetContext sets the context associated with the connection.
func (c *Conn) SetContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *Conn) sepMsgByHeadAndTail() {
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
			log.Ctx(c.ctx).Debug().Msgf("[CutMsgByHeadAndTail] Conn fd:%d  pos:%d msg:%x", c.fd, readPos, data)
			readPos = 0
		}
	}
}

func (c *Conn) sepMsgByTail() {
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
	buf := make([]byte, c.limiter.BufSize.ReadBufSize)
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
		if c.shouldSepByHeadAndTail() {
			c.sepMsgByHeadAndTail()
		}
		if c.shouldSepByTail() {
			c.sepMsgByTail()
		}
	}
}

func (c *Conn) shouldSepByTail() bool {
	headNotAv := c.limiter.Packet.Head == 0
	tailAv := c.limiter.Packet.Tail != 0
	cutByTail := c.limiter.Packet.SepType == SepByTail
	return headNotAv && tailAv && cutByTail
}

func (c *Conn) shouldSepByHeadAndTail() bool {
	headAv := c.limiter.Packet.Head != 0
	tailAv := c.limiter.Packet.Tail != 0
	cutByHeadAndTail := c.limiter.Packet.SepType == SepByHeadAndTail
	return headAv && tailAv && cutByHeadAndTail
}

func (c *Conn) Read(b []byte) (n int, err error) {
	n, err = unix.Read(c.fd, b)
	return
}

// Write writes a message to the connection.
func (c *Conn) Write(message []byte) (int, error) {
	n, err := unix.Write(c.fd, message)
	if err != nil {
		log.Error().Msgf("Conn fd:%d Write error:%+v", c.fd, err)
	}
	c.updated = time.Now().Unix()
	return n, err
}

func (c *Conn) Close() error {
	return unix.Close(c.fd)
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

func (c *Conn) SetLocalAddr(addr net.Addr) {
	c.lAddr = addr
}
