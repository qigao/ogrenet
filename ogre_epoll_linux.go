package ogrenet

import (
	"fmt"
	"net"
	"sync"

	"github.com/qigao/ogrenet/shared/sockaddr"
	"github.com/qigao/ogrenet/shared/sockaddr/netaddr"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

type OgreEpoll struct {
	listenFd   int
	epollId    int
	network    string
	ip         string
	port       int
	eventPool  *sync.Pool
	localAddr  net.Addr
	remoteAddr net.Addr
}

// NewOgreEpoll 初始化epoll 包含创建socket,监听端口，以及创建epoll监听
func NewOgreEpoll(network string, ip string, port int) *OgreEpoll {
	ep := &OgreEpoll{
		eventPool: &sync.Pool{
			New: func() interface{} {
				return make([]unix.EpollEvent, 1024)
			},
		},
	}
	ep.ip = ip
	ep.port = port
	ep.network = network
	return ep
}

func (e *OgreEpoll) Listen() error {
	address := fmt.Sprintf("%s:%d", e.ip, e.port)
	netAddr, err := sockaddr.ResolveAddr(e.network, address)
	if err != nil {
		log.Fatal().Msgf("resolve addr err:%v", err)
	}
	e.localAddr = netAddr
	soAddr := netaddr.NetAddrToSockaddr(netAddr)
	domain := netaddr.NetAddrAF(netAddr)
	sockType := netaddr.NetAddrSOCK(netAddr)
	proto := netaddr.NetAddrIPPROTO(netAddr)
	fd, err := unix.Socket(domain, sockType, proto)
	if err != nil {
		log.Err(err).Msgf("socket error")
		return err
	}
	err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
	if err != nil {
		log.Err(err).Msgf("setsockopt error")
		return err
	}

	err = unix.Bind(fd, soAddr)
	if err != nil {
		log.Err(err).Msgf("bind error")
		return err
	}
	err = unix.Listen(fd, 1024)
	if err != nil {
		log.Err(err).Msgf("listen error")
		return err
	}

	epollFD, err := unix.EpollCreate1(0)
	if err != nil {
		log.Err(err).Msgf("epoll_create1 error")
		return err
	}
	e.epollId = epollFD
	e.listenFd = fd
	e.add(fd)
	return nil
}

func (e *OgreEpoll) Accept(fd int) (int, error) {
	nfd, sa, err := unix.Accept(fd)
	if err != nil {
		log.Error().Err(err).Msgf("accept error,fd is %d", fd)
		return 0, err
	}
	// 设置fd为非阻塞
	if err := unix.SetNonblock(nfd, true); err != nil {
		log.Err(err).Msgf("set nonblock error,fd is %d", fd)
		return nfd, err
	}
	remoteAddr := sockaddr.SockaddrToAddr(sa)
	e.remoteAddr = remoteAddr
	log.Info().Msgf("accept new connection addr is %v", remoteAddr)
	return nfd, e.add(nfd)
}

func (e *OgreEpoll) add(fd int) error {
	event := unix.EpollEvent{Events: EpollListener, Fd: int32(fd)}
	if err := unix.EpollCtl(e.epollId, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		log.Error().Msgf("epoll_ctl add err:%+v,fd:%+v", err, fd)
		return err
	}
	return nil
}

func (e *OgreEpoll) Del(fd int) error {
	event := unix.EpollEvent{Events: EpollListener, Fd: int32(fd)}
	if err := unix.EpollCtl(e.epollId, unix.EPOLL_CTL_DEL, fd, &event); err != nil {
		log.Error().Msgf("epoll_ctl del err:%+v,fd:%+v", err, fd)
		return err
	}
	return nil
}

// Wait epoll wait all events
func (e *OgreEpoll) Wait(handle func(fd int, connType ConnStatus)) error {
	events := e.eventPool.Get().([]unix.EpollEvent)
	defer func() {
		events := make([]unix.EpollEvent, DefaultPacketSize)
		e.eventPool.Put(events)
	}()
	n, err := unix.EpollWait(e.epollId, events[:], -1)
	if err != nil {
		log.Error().Msgf("epoll_wait err:%+v", err)
		return err
	}
	for i := 0; i < n; i++ {
		connType := ConnMessage // 默认是读内容
		if int(events[i].Fd) == e.listenFd {
			connType = ConnNew
		}
		handle(int(events[i].Fd), connType)
	}
	return nil
}

func (e *OgreEpoll) RemoteAddr() net.Addr {
	return e.remoteAddr
}

func (e *OgreEpoll) LocalAddr() net.Addr {
	return e.localAddr
}

func (e *OgreEpoll) Close() error {
	e.Del(e.listenFd)
	return unix.Close(e.epollId)
}
