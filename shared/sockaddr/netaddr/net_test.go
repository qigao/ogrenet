package netaddr

import (
	"bytes"
	"net"
	"testing"
)

func TestIPAddrToSockaddr(t *testing.T) {
	ip := net.ParseIP(TestIPV4)
	addr := &net.IPAddr{IP: ip}
	sa := IPAddrToSockaddr(addr)
	if sa == nil {
		t.Errorf("IPAddrToSockaddr(%v) = nil, want non-nil", addr)
	}
}

func assertIPEq(t *testing.T, ip net.IP, sa Sockaddr) {
	switch s := sa.(type) {
	case *SockaddrInet4:
		if !bytes.Equal(s.Addr[:], ip.To4()) {
			t.Error("IPs not equal")
		}
	case *SockaddrInet6:
		if !bytes.Equal(s.Addr[:], ip.To16()) {
			t.Error("IPs not equal")
		}
	default:
		t.Error("not a known sockaddr")
	}
}

func subtestIPSockaddr(t *testing.T, ip net.IP) {
	assertIPEq(t, ip, IPAndZoneToSockaddr(ip, ""))
}

func TestIPAndZoneToSockaddr(t *testing.T) {
	subtestIPSockaddr(t, net.ParseIP("127.0.0.1"))
	subtestIPSockaddr(t, net.IPv4zero)
	subtestIPSockaddr(t, net.IP(net.IPv4zero.To4()))
	subtestIPSockaddr(t, net.IPv6unspecified)
	assertIPEq(t, net.IPv4zero, IPAndZoneToSockaddr(nil, ""))
}

func TestTCPAddrToSockaddr(t *testing.T) {
	ip := net.ParseIP(TestIPV4)
	addr := &net.TCPAddr{IP: ip, Port: 80}
	sa := TCPAddrToSockaddr(addr)
	if sa == nil {
		t.Errorf("TCPAddrToSockaddr(%v) = nil, want non-nil", addr)
	}
}

func TestUDPAddrToSockaddr(t *testing.T) {
	ip := net.ParseIP(TestIPV4)
	addr := &net.UDPAddr{IP: ip, Port: 80}
	sa := UDPAddrToSockaddr(addr)
	if sa == nil {
		t.Errorf("UDPAddrToSockaddr(%v) = nil, want non-nil", addr)
	}
}

func TestSockaddrToIPAddr(t *testing.T) {
	ip := net.ParseIP(TestIPV4)
	sa := IPAndZoneToSockaddr(ip, "")
	addr := SockaddrToIPAddr(sa)
	if addr == nil {
		t.Errorf("SockaddrToIPAddr(%v) = nil, want non-nil", sa)
	}
}

func TestSockaddrToTCPAddr(t *testing.T) {
	ip := net.ParseIP(TestIPV4)
	sa := IPAndZoneToSockaddr(ip, "")
	sa.(*SockaddrInet4).Port = 80
	addr := SockaddrToTCPAddr(sa)
	if addr == nil {
		t.Errorf("SockaddrToTCPAddr(%v) = nil, want non-nil", sa)
	}
}

func TestSockaddrToUDPAddr(t *testing.T) {
	ip := net.ParseIP(TestIPV4)
	sa := IPAndZoneToSockaddr(ip, "")
	sa.(*SockaddrInet4).Port = 80
	addr := SockaddrToUDPAddr(sa)
	if addr == nil {
		t.Errorf("SockaddrToUDPAddr(%v) = nil, want non-nil", sa)
	}
}

func TestUnixAddrToSockaddr(t *testing.T) {
	addr := &net.UnixAddr{Name: testUnixAddr, Net: "unix"}
	sa, _ := UnixAddrToSockaddr(addr)
	if sa == nil {
		t.Errorf("UnixAddrToSockaddr(%v) = nil, want non-nil", addr)
	}
}

func TestSockaddrToUnixAddr(t *testing.T) {
	sa := &SockaddrUnix{Name: testUnixAddr}
	addr := SockaddrToUnixAddr(sa)
	if addr == nil {
		t.Errorf("SockaddrToUnixAddr(%v) = nil, want non-nil", sa)
	}
}

func TestSockaddrToUnixgramAddr(t *testing.T) {
	sa := &SockaddrUnix{Name: testUnixAddr}
	addr := SockaddrToUnixgramAddr(sa)
	if addr == nil {
		t.Errorf("SockaddrToUnixgramAddr(%v) = nil, want non-nil", sa)
	}
}

func TestSockaddrToUnixpacketAddr(t *testing.T) {
	sa := &SockaddrUnix{Name: testUnixAddr}
	addr := SockaddrToUnixpacketAddr(sa)
	if addr == nil {
		t.Errorf("SockaddrToUnixpacketAddr(%v) = nil, want non-nil", sa)
	}
}

func TestSockaddrToIPAndZone(t *testing.T) {
	ip := net.ParseIP(TestIPV4)
	sa := IPAndZoneToSockaddr(ip, "")
	ip2, zone := SockaddrToIPAndZone(sa)
	if !ip.Equal(ip2) {
		t.Errorf("SockaddrToIPAndZone(%v) = %v, want %v", sa, ip2, ip)
	}
	if zone != "" {
		t.Errorf("SockaddrToIPAndZone(%v) = %v, want \"\"", sa, zone)
	}
}
