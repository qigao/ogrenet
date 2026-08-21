package transport

import (
	"testing"

	"github.com/qigao/ogrenet"
)

func TestObservabilityPublicContracts(t *testing.T) {
	var _ ogrenet.Engine = (*Engine)(nil)
	var _ ogrenet.Listener = (*listener)(nil)
	var _ ogrenet.Session = (*conn)(nil)
	var _ ogrenet.Session = (*wsSession)(nil)
	var _ ogrenet.PacketConn = (*packetConn)(nil)

	var _ ogrenet.Observer = ogrenet.ObserverFunc(func(ogrenet.Event) {})
	_ = ogrenet.Event{Kind: ogrenet.EventRead, Resource: ogrenet.ResourceSession}
	_ = ogrenet.EngineStats{}
	_ = ogrenet.ListenerStats{}
	_ = ogrenet.SessionStats{}
	_ = ogrenet.PacketConnStats{}
}
