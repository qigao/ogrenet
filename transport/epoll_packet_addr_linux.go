//go:build linux

package transport

import (
	"context"
	"errors"
	"net"

	"github.com/qigao/ogrenet"
	"golang.org/x/sys/unix"
)

func resolveNativeUDP(ctx context.Context, endpoint ogrenet.Endpoint, listen bool) (*net.UDPAddr, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if endpoint.Scheme != ogrenet.SchemeUDP {
		return nil, ErrProtocolMismatch
	}
	if endpoint.Host == "" {
		if !listen {
			return nil, errors.New("transport: UDP dial host required")
		}
		return &net.UDPAddr{IP: net.IPv4zero, Port: int(endpoint.Port)}, nil
	}
	if ip := net.ParseIP(endpoint.Host); ip != nil {
		return &net.UDPAddr{IP: append(net.IP(nil), ip...), Port: int(endpoint.Port)}, nil
	}
	addrs, err := (netNativeIPResolver{}).LookupIPAddr(ctx, endpoint.Host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errNativeNoResolvedAddress
	}
	return &net.UDPAddr{IP: append(net.IP(nil), addrs[0].IP...), Port: int(endpoint.Port), Zone: addrs[0].Zone}, nil
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	out := *addr
	out.IP = append(net.IP(nil), addr.IP...)
	return &out
}

func nativeUDPAddrToSockaddr(addr *net.UDPAddr) (unix.Sockaddr, int, error) {
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

func nativeSockaddrToUDPAddr(sa unix.Sockaddr) (*net.UDPAddr, error) {
	switch x := sa.(type) {
	case *unix.SockaddrInet4:
		return &net.UDPAddr{IP: net.IPv4(x.Addr[0], x.Addr[1], x.Addr[2], x.Addr[3]), Port: x.Port}, nil
	case *unix.SockaddrInet6:
		ip := make(net.IP, net.IPv6len)
		copy(ip, x.Addr[:])
		zone := ""
		if x.ZoneId != 0 {
			if iface, err := net.InterfaceByIndex(int(x.ZoneId)); err == nil {
				zone = iface.Name
			}
		}
		return &net.UDPAddr{IP: ip, Port: x.Port, Zone: zone}, nil
	default:
		return nil, errNativeSockaddrType
	}
}

func nativeUDPSocketAddr(fd int, peer bool) (*net.UDPAddr, error) {
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
	return nativeSockaddrToUDPAddr(sa)
}
