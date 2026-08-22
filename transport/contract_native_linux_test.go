//go:build linux

package transport_test

import (
	"testing"
	"time"

	"github.com/qigao/ogrenet"
	"github.com/qigao/ogrenet/transport"
)

func epollFactory(profile contractProfile) engineFactory {
	return engineFactory{
		name:    "epoll",
		profile: profile,
		new: func(t *testing.T, opts ...transport.Option) ogrenet.Engine {
			t.Helper()
			e, err := transport.NewEpoll(transport.EpollConfig{Pollers: 1}, opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			return e
		},
	}
}

func TestEpollFactoryPhase6AConstruction(t *testing.T) {
	factory := epollFactory(contractProfile{})
	if factory.profile.TCP || factory.profile.UDP {
		t.Fatalf("6A epoll profile must advertise no native protocol support: %+v", factory.profile)
	}

	e := factory.new(t)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Done():
	case <-time.After(time.Second):
		t.Fatal("epoll 6A engine did not close its Done barrier")
	}
}

func TestEpollPublicTCPContracts(t *testing.T) {
	runEngineContracts(t, epollFactory(contractProfile{TCP: true}))
}

func TestEpollPublicUDPContracts(t *testing.T) {
	runEngineContracts(t, epollFactory(contractProfile{UDP: true}))
}
