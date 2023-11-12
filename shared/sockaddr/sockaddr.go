package sockaddr

import (
	"net"

	"github.com/qigao/ogrenet/shared/sockaddr/netaddr"
	"golang.org/x/sys/unix"
)

// Socklen is a type for the length of a sockaddr.
type Socklen uint

// SockaddrToAny converts a Sockaddr into a RawSockaddrAny
// The implementation is platform dependent.
func SockaddrToAny(sa netaddr.Sockaddr) (*netaddr.RawSockaddrAny, Socklen, error) {
	return sockaddrToAny(sa)
}

// AnyToSockaddr SockaddrToAny converts a RawSockaddrAny into a Sockaddr
// The implementation is platform dependent.
func AnyToSockaddr(rsa *netaddr.RawSockaddrAny) (netaddr.Sockaddr, error) {
	return anyToSockaddr(rsa)
}

// SockaddrToAddr returns a go/netaddr friendly address
func SockaddrToAddr(sa unix.Sockaddr) net.Addr {
	var addr net.Addr
	switch sa := sa.(type) {
	case *unix.SockaddrInet4:
		addr = &net.TCPAddr{
			IP:   append([]byte{}, sa.Addr[:]...),
			Port: sa.Port,
		}
	case *unix.SockaddrInet6:
		var zone string
		if sa.ZoneId != 0 {
			if ifi, err := net.InterfaceByIndex(int(sa.ZoneId)); err == nil {
				zone = ifi.Name
			}
		}
		// 		if zone == "" && sa.ZoneId != 0 {
		// 			return nil
		// 		}
		addr = &net.TCPAddr{
			IP:   append([]byte{}, sa.Addr[:]...),
			Port: sa.Port,
			Zone: zone,
		}
	case *unix.SockaddrUnix:
		addr = &net.UnixAddr{Net: "unix", Name: sa.Name}
	}
	return addr
}

func ResolveAddr(network, address string) (net.Addr, error) {
	switch network {
	default:
		return nil, net.UnknownNetworkError(network)
	case "ip", "ip4", "ip6":
		return net.ResolveIPAddr(network, address)
	case "tcp", "tcp4", "tcp6":
		return net.ResolveTCPAddr(network, address)
	case "udp", "udp4", "udp6":
		return net.ResolveUDPAddr(network, address)
	case "unix", "unixgram", "unixpacket":
		return net.ResolveUnixAddr(network, address)
	}
}
