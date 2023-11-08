package ogrenet

import (
	"bytes"
	"context"
	"net"
	"syscall"
	"time"

	"github.com/qigao/ogrenet/codecs"

	"github.com/rs/zerolog/log"
)

func NewNetConn(fd int, msgPool *MessagePool, msgChan chan *MsgConn) *Conn {
	conn := &Conn{
		fd:      fd,
		updated: time.Now().Unix(),
		pool:    msgPool,
		ctx:     context.Background(),
	}
	conn.msgChan = msgChan
	conn.limiter.Packet.CutType = CutByHeadAndTail
	conn.limiter.Packet.Head = codecs.DefaultMagicHead
	conn.limiter.Packet.Tail = codecs.DefaultMagicTail
	return conn
}

func NewNetConnWithTerm(fd int, msgPool *MessagePool, msgChan chan *MsgConn, limiter *Limiter) *Conn {
	if limiter == nil {
		return NewNetConn(fd, msgPool, msgChan)
	}
	conn := NewNetConn(fd, msgPool, msgChan)
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
			log.Ctx(c.ctx).Debug().Msgf("[CutMsgByHeadAndTail] Conn fd:%d  pos:%d msg:%x", c.fd, readPos, data)
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
	buf := make([]byte, MaxReadBufSize)
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
	cutByTail := c.limiter.Packet.CutType == CutByTail
	return headNotAv && tailAv && cutByTail
}

func (c *Conn) shouldCutByHeadAndTail() bool {
	headAv := c.limiter.Packet.Head != 0
	tailAv := c.limiter.Packet.Tail != 0
	cutByHeadAndTail := c.limiter.Packet.CutType == CutByHeadAndTail
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

func (c *Conn) SetLocalAddr(addr net.Addr) {
	c.lAddr = addr
}

func (c *Conn) PushData(dst int, data []byte) {
	c.ctx = context.WithValue(c.ctx, PushKey{}, PushData{
		data: data,
		fd:   dst,
	})
}

func (c *Conn) getPushData() PushData {
	value := c.ctx.Value(PushKey{})
	if value == nil {
		return PushData{}
	}
	return value.(PushData)
}

func (c *Conn) PubData(dest []int, data []byte) {
	c.ctx = context.WithValue(c.ctx, PubKey{}, PubData{
		data: data,
		fd:   dest,
	})
}

func (c *Conn) getPubData() PubData {
	value := c.ctx.Value(PubKey{})
	if value == nil {
		return PubData{}
	}
	return value.(PubData)
}

func (c *Conn) SetMode(mode WorkMode) {
	c.ctx = context.WithValue(c.ctx, ModeKey{}, mode)
}

func (c *Conn) getMode() WorkMode {
	cfg := c.ctx.Value(ModeKey{})
	if cfg == nil {
		return UnknowMode
	} else {
		return cfg.(WorkMode)
	}
}

func (c *Conn) ForwardData(data []byte) {
	c.ctx = context.WithValue(c.ctx, ForwardKey{}, data)
}

func (c *Conn) getForwardData() []byte {
	return c.ctx.Value(ForwardKey{}).([]byte)
}
