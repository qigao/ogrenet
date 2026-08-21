package ogrenet

import (
	"net"
	"time"
)

type ResourceKind uint8

const (
	ResourceEngine ResourceKind = iota + 1
	ResourceListener
	ResourceSession
	ResourcePacketConn
)

type EventKind uint8

const (
	EventAccept EventKind = iota + 1
	EventConnect
	EventHandshake
	EventRead
	EventWrite
	EventBackpressure
	EventDrop
	EventClose
)

type Event struct {
	Kind       EventKind
	Resource   ResourceKind
	ResourceID uint64
	ParentID   uint64
	Protocol   Scheme
	Local      net.Addr
	Remote     net.Addr
	Bytes      uint64
	Duration   time.Duration
	Err        error
}

type Observer interface {
	Observe(Event)
}

type ObserverFunc func(Event)

func (f ObserverFunc) Observe(event Event) {
	if f != nil {
		f(event)
	}
}
