//go:build linux

package transport

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/qigao/ogrenet"
	"golang.org/x/sys/unix"
)

type nativeIPResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type netNativeIPResolver struct{}

func (netNativeIPResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}

var (
	errNativeInvalidTCPAddress = errors.New("transport: invalid native tcp address")
	errNativeNoResolvedAddress = errors.New("transport: native tcp address resolved to no addresses")
	errNativeSockaddrType       = errors.New("transport: unsupported native socket address")
)

func resolveNativeListenTCP(ctx context.Context, endpoint ogrenet.Endpoint, resolver nativeIPResolver) (*net.TCPAddr, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if endpoint.Scheme != ogrenet.SchemeTCP {
		return nil, ErrProtocolMismatch
	}
	if endpoint.Host == "" {
		return &net.TCPAddr{IP: net.IPv4zero, Port: int(endpoint.Port)}, nil
	}
	if ip := net.ParseIP(endpoint.Host); ip != nil {
		return &net.TCPAddr{IP: ip, Port: int(endpoint.Port)}, nil
	}
	if resolver == nil {
		resolver = netNativeIPResolver{}
	}
	addrs, err := resolver.LookupIPAddr(ctx, endpoint.Host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errNativeNoResolvedAddress
	}
	return &net.TCPAddr{IP: append(net.IP(nil), addrs[0].IP...), Port: int(endpoint.Port), Zone: addrs[0].Zone}, nil
}

func nativeTCPAddrToSockaddr(addr *net.TCPAddr) (unix.Sockaddr, int, error) {
	if addr == nil || addr.Port < 0 || addr.Port > 65535 {
		return nil, 0, errNativeInvalidTCPAddress
	}
	ip := addr.IP
	if len(ip) == 0 {
		ip = net.IPv4zero
	}
	if v4 := ip.To4(); v4 != nil {
		sa := &unix.SockaddrInet4{Port: addr.Port}
		copy(sa.Addr[:], v4)
		return sa, unix.AF_INET, nil
	}
	v6 := ip.To16()
	if v6 == nil {
		return nil, 0, errNativeInvalidTCPAddress
	}
	sa := &unix.SockaddrInet6{Port: addr.Port}
	copy(sa.Addr[:], v6)
	if addr.Zone != "" {
		iface, err := net.InterfaceByName(addr.Zone)
		if err != nil {
			return nil, 0, err
		}
		sa.ZoneId = uint32(iface.Index)
	}
	return sa, unix.AF_INET6, nil
}

func nativeSockaddrToTCPAddr(sa unix.Sockaddr) (*net.TCPAddr, error) {
	switch x := sa.(type) {
	case *unix.SockaddrInet4:
		return &net.TCPAddr{IP: net.IPv4(x.Addr[0], x.Addr[1], x.Addr[2], x.Addr[3]), Port: x.Port}, nil
	case *unix.SockaddrInet6:
		ip := make(net.IP, net.IPv6len)
		copy(ip, x.Addr[:])
		zone := ""
		if x.ZoneId != 0 {
			if iface, err := net.InterfaceByIndex(int(x.ZoneId)); err == nil {
				zone = iface.Name
			}
		}
		return &net.TCPAddr{IP: ip, Port: x.Port, Zone: zone}, nil
	default:
		return nil, errNativeSockaddrType
	}
}

func nativeSocketAddr(fd int, peer bool) (*net.TCPAddr, error) {
	var (
		sa  unix.Sockaddr
		err error
	)
	if peer {
		sa, err = unix.Getpeername(fd)
	} else {
		sa, err = unix.Getsockname(fd)
	}
	if err != nil {
		return nil, err
	}
	return nativeSockaddrToTCPAddr(sa)
}

func configureNativeTCP(fd int, cfg TCPConfig) error {
	noDelay := 0
	if cfg.NoDelay {
		noDelay = 1
	}
	if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, noDelay); err != nil {
		return err
	}

	keepAlive := 0
	if cfg.KeepAlive {
		keepAlive = 1
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_KEEPALIVE, keepAlive); err != nil {
		return err
	}
	if cfg.KeepAlive && cfg.KeepAlivePeriod > 0 {
		seconds := int((cfg.KeepAlivePeriod + time.Second - 1) / time.Second)
		if seconds < 1 {
			seconds = 1
		}
		if err := unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, seconds); err != nil {
			return err
		}
	}
	if cfg.ReadBufferBytes > 0 {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, cfg.ReadBufferBytes); err != nil {
			return err
		}
	}
	if cfg.WriteBufferBytes > 0 {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, cfg.WriteBufferBytes); err != nil {
			return err
		}
	}
	return nil
}
