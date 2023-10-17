package sockaddr

import (
	"errors"
	"net"
	"testing"
)

func TestResolveAddr(t *testing.T) {
	testCases := []struct {
		name     string
		network  string
		address  string
		expected net.Addr
		err      error
	}{
		{
			name:     "resolve tcp address",
			network:  "tcp",
			address:  "localhost:8080",
			expected: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080},
			err:      nil,
		},
		{
			name:     "resolve udp address",
			network:  "udp",
			address:  "localhost:8080",
			expected: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080},
			err:      nil,
		},
		{
			name:     "resolve unix address",
			network:  "unix",
			address:  "/var/run/mysocket.sock",
			expected: &net.UnixAddr{Net: "unix", Name: "/var/run/mysocket.sock"},
			err:      nil,
		},
		{
			name:     "unknown network",
			network:  "foo",
			address:  "bar",
			expected: nil,
			err:      net.UnknownNetworkError("foo"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := ResolveAddr(tc.network, tc.address)
			if !errors.Is(err, tc.err) {
				t.Errorf("expected error %v, but got %v", tc.err, err)
			}
			if !netAddrsEqual(actual, tc.expected) {
				t.Errorf("expected %v, but got %v", tc.expected, actual)
			}
		})
	}
}

func netAddrsEqual(a, b net.Addr) bool {
	if a == nil || b == nil {
		return a == b
	}
	switch a := a.(type) {
	case *net.TCPAddr:
		b, ok := b.(*net.TCPAddr)
		if !ok {
			return false
		}
		return a.IP.Equal(b.IP) && a.Port == b.Port
	case *net.UDPAddr:
		b, ok := b.(*net.UDPAddr)
		if !ok {
			return false
		}
		return a.IP.Equal(b.IP) && a.Port == b.Port
	case *net.UnixAddr:
		b, ok := b.(*net.UnixAddr)
		if !ok {
			return false
		}
		return a.Name == b.Name
	default:
		return false
	}
}
