//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly

package network

import (
	"errors"
	"runtime"
	"sync/atomic"
	"syscall"

	"github.com/rs/zerolog/log"
)

type NetPoll struct {
	ogreNet  *OgreNet
	index    int
	fd       int
	wfd      int
	shutdown atomic.Bool
}

func NewNetPoll(oNet *OgreNet) (*NetPoll, error) {
	p := new(NetPoll)
	p.ogreNet = oNet

	fd, err := syscall.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	p.fd = fd

	r1, _, err0 := syscall.Syscall(syscall.SYS_EVENTFD2, 0, syscall.O_NONBLOCK, 0)
	if err0 != 0 {
		syscall.Close(p.fd)
		return nil, err0
	}
	p.wfd = int(r1)

	if err = syscall.EpollCtl(fd, syscall.EPOLL_CTL_ADD, int(r1),
		&syscall.EpollEvent{
			Fd:     int32(r1),
			Events: syscall.EPOLLIN,
		},
	); err != nil {
		syscall.Close(p.fd)
		syscall.Close(p.wfd)
		return nil, err
	}

	return p, nil
}

func (m *NetPoll) Wait() error {
	mesc := -1
	events := make([]syscall.EpollEvent, 1024)

	handler := m.ogreNet.GetEventHandler()

	for !m.shutdown.Load() {
		eventsNum, err := syscall.EpollWait(m.fd, events, mesc)
		if err != nil && errors.Is(err, syscall.EINTR) {
			return err
		}
		// no event
		if eventsNum <= 0 {
			mesc = -1
			runtime.Gosched()
			continue
		}
		mesc = 20

		for i := 0; i < eventsNum; i++ {
			event := events[i]

			// find OgreConn
			c := m.ogreNet.GetConn(int(event.Fd))
			if c == nil {
				syscall.Close(int(event.Fd))
				continue
			}

			// 写
			if event.Events&WriteEvents != 0 {
				log.Info().Msgf("fd:%d write event", event.Fd)
				c.Flush()
			}

			// event Error
			if event.Events&ErrEvents != 0 {
				m.closeConn(c)
				continue
			}

			// read event
			if event.Events&ReadEvents != 0 {
				handler.OnRead(c.Context(), c)
			}
		}
	}

	return nil
}

func (m *NetPoll) closeConn(c Conn) {
	if c == nil {
		return
	}

	c.Close()
	m.removeConn(c)

	e := m.ogreNet.GetEventHandler()
	if e != nil {
		e.OnClose(c.Context(), c)
	}
}

func (m *NetPoll) removeConn(c Conn) {
	if c == nil {
		return
	}

	fd := c.Fd()
	if c := m.ogreNet.GetConn(fd); c != nil {
		m.ogreNet.Remove(fd)
		_ = m.Delete(fd)
	}
}

func (m *NetPoll) Close() error {
	m.shutdown.Store(true)
	syscall.Close(m.wfd)
	return syscall.Close(m.fd)
}

func (m *NetPoll) AddRead(fd int) error {
	return syscall.EpollCtl(m.fd, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{
		Fd:     int32(fd),
		Events: syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP | syscall.EPOLLPRI | syscall.EPOLLIN,
	})
}

func (m *NetPoll) ModWrite(fd int) error {
	return syscall.EpollCtl(m.fd, syscall.EPOLL_CTL_MOD, fd, &syscall.EpollEvent{
		Fd:     int32(fd),
		Events: syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP | syscall.EPOLLPRI | syscall.EPOLLIN | syscall.EPOLLOUT,
	})
}

func (m *NetPoll) ModRead(fd int) error {
	return syscall.EpollCtl(m.fd, syscall.EPOLL_CTL_MOD, fd, &syscall.EpollEvent{
		Fd:     int32(fd),
		Events: syscall.EPOLLERR | syscall.EPOLLHUP | syscall.EPOLLRDHUP | syscall.EPOLLPRI | syscall.EPOLLIN,
	})
}

func (m *NetPoll) Delete(fd int) error {
	return syscall.EpollCtl(m.fd, syscall.EPOLL_CTL_DEL, fd, &syscall.EpollEvent{Fd: int32(fd)})
}
