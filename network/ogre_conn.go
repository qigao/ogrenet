package network

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/qigao/ogrenet/utils"

	"github.com/pkg/errors"
	"github.com/qigao/ogrenet/buffer"
)

// var _ OgreConn = (*OgreConn)(nil)
type OgreConn struct {
	ctx      context.Context
	fd       int
	laddr    net.Addr
	raddr    net.Addr
	isClosed atomic.Bool
	mux      sync.RWMutex
	rBuf     *buffer.SpscRingBuffer
	wBuf     *buffer.SpscRingBuffer
	poller   *NetPoll
}

func NewOgreConn(fd int, local, remote net.Addr) *OgreConn {
	return &OgreConn{
		fd:    fd,
		laddr: local,
		raddr: remote,
		ctx:   context.Background(),
	}
}

func (c *OgreConn) LocalAddr() net.Addr {
	return c.laddr
}

func (c *OgreConn) RemoteAddr() net.Addr {
	return c.raddr
}

func (c *OgreConn) Context() context.Context {
	return c.ctx
}

func (c *OgreConn) write(b []byte) (int, error) {
	return syscall.Write(c.fd, b)
}

func (c *OgreConn) Flush() error {
	if c.isClosed.Load() {
		return net.ErrClosed
	}
	for i := 0; i < c.wBuf.Length(); i++ {
		v, err := c.wBuf.Dequeue()
		if err != nil {
			return err
		}
		b, _ := v.([]byte)
		_, err = c.write(b)
		if err != nil && !errors.Is(err, syscall.EINTR) && !errors.Is(err, syscall.EAGAIN) {
			_ = c.Close()
			c.poller.removeConn(c)
			return err
		}
	}
	// reset to read
	c.resetRead()
	return nil
}

func (c *OgreConn) resetRead() {
	if !c.isClosed.Load() {
		c.poller.ModRead(c.fd)
	}
}

func (c *OgreConn) Read(b []byte) (n int, err error) {
	n, err = syscall.Read(c.fd, b)
	return
}

func (c *OgreConn) Fd() int {
	return c.fd
}

func (c *OgreConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	bufLen := 0
	buf := utils.SplitSliceBySep(b, []byte(NewLine))
	for _, v := range buf {
		err := c.wBuf.Enqueue(v)
		if errors.Is(err, buffer.ErrIsFull) {
			return bufLen, err
		}
		bufLen += len(v)
	}
	go c.Flush()
	return bufLen, nil
}

func (c *OgreConn) Close() error {
	c.isClosed.Store(true)
	return syscall.Close(c.fd)
}

// SetDeadline unSupport
func (c *OgreConn) SetDeadline(t time.Time) error {
	return nil
}

// SetReadDeadline unSupport
func (c *OgreConn) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline unSupport
func (c *OgreConn) SetWriteDeadline(t time.Time) error {
	return nil
}
