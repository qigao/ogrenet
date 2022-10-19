package network

import (
	"net"
	"os"
	"sync"
	"syscall"

	"github.com/qigao/ogrenet/sockaddr"

	"github.com/dsnet/try"
	"github.com/rs/zerolog/log"
)

type OgreEpoll struct {
	socket     int        // socket连接
	epId       int        // epoll 创建的唯一描述符
	ip         string     // socket监听的地址
	port       int        // socket监听的端口
	eventPool  *sync.Pool // 接收epoll消息
	localAddr  net.Addr
	remoteAddr net.Addr
}

// NewOgreEpoll 初始化epoll 包含创建socket,监听端口，以及创建epoll监听
func NewOgreEpoll(ip string, port int) *OgreEpoll {
	ep := &OgreEpoll{
		eventPool: &sync.Pool{New: func() interface{} { return make([]syscall.EpollEvent, 1024) }},
	}
	ep.ip = ip
	ep.port = port
	return ep.Setup().listen().getGlobalFd()
}

// Setup 创建socket对象
// TODO: #7 setup socket and option by ip&port from sockaddr
func (e *OgreEpoll) Setup() *OgreEpoll {
	/*第一个参数 domain
	  syscall.AF_INET，表示服务器之间的网络通信
	  syscall.AF_UNIX表示同一台机器上的进程通信
	  syscall.AF_INET6表示以IPv6的方式进行服务器之间的网络通信
	*/
	/*第二个参数 type
	  syscall.SOCK_RAW，表示使用原始套接字，可以构建传输层的协议头部，启用IP_HDRINCL的话，IP层的协议头部也可以构造，就是上面区分的传输层socket和网络层socket。
	  syscall.SOCK_STREAM, 基于TCP的socket通信，应用层socket。
	  syscall.SOCK_DGRAM, 基于UDP的socket通信，应用层socket。
	*/
	/* 第三个参数 proto
	IPPROTO_TCP 接收TCP协议的数据
	IPPROTO_IP 接收任何的IP数据包
	IPPROTO_UDP 接收UDP协议的数据
	IPPROTO_ICMP 接收ICMP协议的数据
	IPPROTO_RAW 只能用来发送IP数据包，不能接收数据。
	*/
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		log.Fatal().Msgf("Setup err:%v", err)
	}
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		log.Fatal().Msgf("set ReuseAddr failed,err:%v", err)
	}
	e.socket = fd
	return e
}

func (e *OgreEpoll) listen() *OgreEpoll {
	if err := syscall.SetNonblock(e.socket, true); err != nil {
		log.Fatal().Msgf("setnonblock err:%v", err)
	}
	q := try.E1(sockaddr.ResolveAddr("ip", e.ip))

	e.localAddr = q
	addr := syscall.SockaddrInet4{Port: e.port}
	ip := "0.0.0.0"
	if e.ip != "" {
		ip = e.ip
	}
	copy(addr.Addr[:], net.ParseIP(ip).To4())
	if err := syscall.Bind(e.socket, &addr); err != nil {
		log.Fatal().Err(err).Msgf("bind err:%v", err)
	}
	// addr := netaddr.NetAddrToSockaddr(q)
	// if err := syscall.Bind(e.socket, addr.(unix.Sockaddr)); err != nil {
	// 	log.Error().Msgf("bind err:%v", err)
	// 	os.Exit(1)
	// }
	if err := syscall.Listen(e.socket, 10); err != nil {
		log.Error().Msgf("listen err:%v", err)
		os.Exit(1)
	}
	return e
}

func (e *OgreEpoll) Accept(fd int) (int, error) {
	nfd, sa, err := syscall.Accept(fd)
	if err != nil {
		log.Error().Err(err).Msgf("accept error,fd is %d", fd)
		return 0, err
	}
	// 设置fd为非阻塞
	if err := syscall.SetNonblock(nfd, true); err != nil {
		log.Err(err).Msgf("set nonblock error,fd is %d", fd)
		return nfd, err
	}
	addr := sockaddr.SockaddrToAddr(sa)
	e.remoteAddr = addr
	log.Info().Msgf("accept new connection addr is %v", addr)
	return nfd, e.add(nfd)
}

func (e *OgreEpoll) getGlobalFd() *OgreEpoll {
	epfd, err := syscall.EpollCreate1(0)
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
	if err := syscall.EpollCtl(e.epId, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{Events: EpollListener, Fd: int32(fd)}); err != nil {
		log.Error().Msgf("epoll_ctl add err:%+v,fd:%+v", err, fd)
		return err
	}
	return nil
}

func (e *OgreEpoll) Del(fd int) error {
	if err := syscall.EpollCtl(e.epId, syscall.EPOLL_CTL_DEL, fd, &syscall.EpollEvent{Events: EpollListener, Fd: int32(fd)}); err != nil {
		log.Error().Msgf("epoll_ctl del err:%+v,fd:%+v", err, fd)
		return err
	}
	return nil
}

// Wait epoll wait all events
func (e *OgreEpoll) Wait(handle func(fd int, connType ConnStatus)) error {
	events := e.eventPool.Get().([]syscall.EpollEvent)
	defer func() {
		events := make([]syscall.EpollEvent, 1024)
		e.eventPool.Put(events)
	}()
	n, err := syscall.EpollWait(e.epId, events[:], -1)
	if err != nil {
		log.Error().Msgf("epoll_wait err:%+v", err)
		return err
	}
	for i := 0; i < n; i++ {
		// 如果是系统描述符，就建立一个新的连接
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
	return syscall.Close(e.epId)
}
