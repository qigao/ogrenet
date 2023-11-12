package ogrenet

import (
	"fmt"
	"net"
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/qigao/ogrenet/shared/sockaddr"
	"github.com/qigao/ogrenet/shared/sockaddr/netaddr"

	"github.com/dsnet/try"
	"github.com/rs/zerolog/log"
)

type OgreEpoll struct {
	socket     int
	epId       int
	ip         string
	port       int
	eventPool  *sync.Pool
	localAddr  net.Addr
	remoteAddr net.Addr
}

// NewOgreEpoll 初始化epoll 包含创建socket,监听端口，以及创建epoll监听
func NewOgreEpoll(ip string, port int) *OgreEpoll {
	ep := &OgreEpoll{
		eventPool: &sync.Pool{New: func() interface{} { return make([]unix.EpollEvent, 1024) }},
	}
	ep.ip = ip
	ep.port = port
	return ep.Setup().listen().getGlobalFd()
}

// Setup 创建socket对象
// TODO: #7 setup socket and option by ip&port from sockaddr
func (e *OgreEpoll) Setup() *OgreEpoll {
	/*第一个参数 domain
	  unix.AF_INET，表示服务器之间的网络通信
	  unix.AF_UNIX表示同一台机器上的进程通信
	  unix.AF_INET6表示以IPv6的方式进行服务器之间的网络通信
	*/
	/*第二个参数 type
	  unix.SOCK_RAW，表示使用原始套接字，可以构建传输层的协议头部，启用IP_HDRINCL的话，IP层的协议头部也可以构造，就是上面区分的传输层socket和网络层socket。
	  unix.SOCK_STREAM, 基于TCP的socket通信，应用层socket。
	  unix.SOCK_DGRAM, 基于UDP的socket通信，应用层socket。
	*/
	/* 第三个参数 proto
	IPPROTO_TCP 接收TCP协议的数据
	IPPROTO_IP 接收任何的IP数据包
	IPPROTO_UDP 接收UDP协议的数据
	IPPROTO_ICMP 接收ICMP协议的数据
	IPPROTO_RAW 只能用来发送IP数据包，不能接收数据。
	*/
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, unix.IPPROTO_TCP)
	if err != nil {
		log.Fatal().Msgf("Setup err:%v", err)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		log.Fatal().Msgf("set ReuseAddr failed,err:%v", err)
	}
	e.socket = fd
	return e
}

func (e *OgreEpoll) listen() *OgreEpoll {
	if err := unix.SetNonblock(e.socket, true); err != nil {
		log.Fatal().Msgf("setnonblock err:%v", err)
	}
	q := try.E1(sockaddr.ResolveAddr("ip", e.ip))
	e.localAddr = q
	addrd := e.resolveSockAddr4()
	if err := unix.Bind(e.socket, addrd); err != nil {
		log.Error().Msgf("bind err:%v", err)
		os.Exit(1)
	}
	if err := unix.Listen(e.socket, 10); err != nil {
		log.Error().Msgf("listen err:%v", err)
		os.Exit(1)
	}
	return e
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
	addr := netaddr.SockaddrToTCPAddr(sa)
	e.remoteAddr = addr
	log.Info().Msgf("accept new connection addr is %v", addr)
	return nfd, e.add(nfd)
}

func (e *OgreEpoll) getGlobalFd() *OgreEpoll {
	epfd, err := unix.EpollCreate1(0)
	log.Info().Msgf("GlobalFd epfd：%+v,e.fd:%d", epfd, e.socket)
	if err != nil {
		log.Error().Msgf("epoll_create1 err:%+v", err)
		os.Exit(1)
	}
	e.epId = epfd
	e.add(e.socket)
	return e
}

func (e *OgreEpoll) add(fd int) error {
	event := unix.EpollEvent{Events: EpollListener, Fd: int32(fd)}
	if err := unix.EpollCtl(e.epId, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		log.Error().Msgf("epoll_ctl add err:%+v,fd:%+v", err, fd)
		return err
	}
	return nil
}

func (e *OgreEpoll) Del(fd int) error {
	event := unix.EpollEvent{Events: EpollListener, Fd: int32(fd)}
	if err := unix.EpollCtl(e.epId, unix.EPOLL_CTL_DEL, fd, &event); err != nil {
		log.Error().Msgf("epoll_ctl del err:%+v,fd:%+v", err, fd)
		return err
	}
	return nil
}

// Wait epoll wait all events
func (e *OgreEpoll) Wait(handle func(fd int, connType ConnStatus)) error {
	events := e.eventPool.Get().([]unix.EpollEvent)
	defer func() {
		events := make([]unix.EpollEvent, 1024)
		e.eventPool.Put(events)
	}()
	n, err := unix.EpollWait(e.epId, events[:], -1)
	if err != nil {
		log.Error().Msgf("epoll_wait err:%+v", err)
		return err
	}
	for i := 0; i < n; i++ {
		connType := ConnMessage // 默认是读内容
		if int(events[i].Fd) == e.socket {
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
	return unix.Close(e.epId)
}

func (e *OgreEpoll) resolveSockAddr4() unix.Sockaddr {
	ipaddr := fmt.Sprintf("%s:%d", e.ip, e.port)
	addr, err := net.ResolveTCPAddr("tcp4", ipaddr)
	if err != nil {
		return nil
	}
	ip := addr.IP
	if len(ip) == 0 {
		ip = net.IPv4zero
	}
	sa4 := &unix.SockaddrInet4{Port: addr.Port}
	copy(sa4.Addr[:], ip.To4())
	return sa4
}
