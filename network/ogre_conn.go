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
	UpdateTime int64 // 最新的更新时间，判断超时用
	ctx        interface{}
	remoteAddr net.Addr
	localAddr  net.Addr
	bytePool   *MessagePool
}

func NewConn(fd int, msgPool *MessagePool) *Conn {
	conn := &Conn{
		fd:         fd,
		UpdateTime: time.Now().Unix(),
		bytePool:   msgPool,
		ctx:        context.Background(),
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
	//<-c.ready
	buf := c.bytePool.BytePool.Get().([]byte)
	defer func() {
		// buf = make([]byte, MaxPacketSize)
		c.bytePool.BytePool.Put(buf)
	}()
	readPos := 0
	for !c.bytePool.RBuf.IsEmpty() {
		b, _ := c.bytePool.RBuf.ReadByte()
		if b == modbus.MagicHead {
			readPos++
		}
		if readPos > 0 {
			buf = append(buf, b)
		}
		if b == modbus.MagicTail {
			log.Info().Msgf("Conn ReadWorker received message:%x", buf)
			data := bytes.Trim(buf, "\x00")
			MessageChan <- &MsgConn{c, data}
			readPos = 0
		}
	}
	// close(c.ready)
	// ClosedConn <- c
}

func (c *Conn) ReadToMemory() {
	buf := c.bytePool.NetRBuf.Get().([]byte)
	defer func() {
		buf = make([]byte, MaxPacketSize)
		c.bytePool.NetRBuf.Put(buf)
	}()
	n, err := c.Read(buf)
	if err != nil {
		log.Error().Msgf("read error,fd is %d", c.fd)
		c.Close()
		return
	}
	if n > 0 {
		wn, err := c.bytePool.RBuf.Write(buf[:n])
		log.Debug().Msgf("write to rbuf, wn:%d, err:%+v", wn, err)
		// c.ready <- struct{}{}
		c.UpdateTime = time.Now().Unix()
		log.Debug().Msgf("Conn ReadToMemory received message:%x", buf[:n])
		go c.ReadWorker()
		// OpenedConn <- c
	}
}

func (c *Conn) Read(b []byte) (n int, err error) {
	n, err = syscall.Read(c.fd, b)
	if err != nil {
		log.Error().Msgf("Conn %d Read received  n:%d, err:%+v", c.fd, n, err)
	}
	return
}

// Write writes a message to the connection.
func (c *Conn) Write(message []byte) (int, error) {
	n, err := syscall.Write(c.fd, message)
	log.Info().Msgf("Conn Write message:%x, n:%d, err:%+v", message, n, err)
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
