package network

import (
	"bytes"
	"context"
	"net"
	"syscall"
	"time"

	"github.com/qigao/ogrenet/codecs/modbus"
	"github.com/rs/zerolog/log"
)

type Conn struct {
	fd         int   // 当前连接的文件描述符 Fd
	Updated    int64 // 最新的更新时间，判断超时用
	ctx        interface{}
	remoteAddr net.Addr
	localAddr  net.Addr
	msg        *MessagePool
}

func NewNetConn(fd int, msgPool *MessagePool) *Conn {
	conn := &Conn{
		fd:      fd,
		Updated: time.Now().Unix(),
		msg:     msgPool,
		ctx:     context.Background(),
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

func (c *Conn) ReadWorker() {
	buf := c.msg.BytePool.Get().([]byte)
	defer func() {
		buf = make([]byte, MaxPacketSize)
		c.msg.BytePool.Put(buf)
	}()
	readPos := 0
	for !c.msg.RBuf.IsEmpty() {
		b, _ := c.msg.RBuf.ReadByte()
		if b == modbus.MagicHead {
			readPos++
		}
		if readPos > 0 {
			buf = append(buf, b)
		}
		if b == modbus.MagicTail {
			data := bytes.Trim(buf, "\x00")
			log.Info().Msgf("Conn fd:%d ReadWorker will process:%x", c.fd, data)
			MessageChan <- &MsgConn{c, data}
			readPos = 0
		}
	}
}

func (c *Conn) ReadAll() {
	buf := c.msg.NetRBuf.Get().([]byte)
	defer func() {
		buf = make([]byte, MaxPacketSize)
		c.msg.NetRBuf.Put(buf)
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
		go c.ReadWorker()
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
