package ogrenet

import (
	"net"
	"time"
)

type EngineStats struct {
	OpeningConnections  uint64
	ActiveConnections   uint64
	DrainingConnections uint64
	ActiveHandshakes    uint64
	PendingUpgrades     uint64
	GlobalQueuedBytes   uint64

	RejectedConnections uint64
	RejectedPeers       uint64
	RejectedListeners   uint64
	RejectedHandshakes  uint64
	RejectedUpgrades    uint64
	RejectedQueuedBytes uint64

	ObserverDroppedEvents uint64
	ObserverPanics        uint64
}

type SessionStats struct {
	ResourceID uint64
	Protocol   Scheme
	Local      net.Addr
	Remote     net.Addr
	Age        time.Duration

	BytesRX    uint64
	BytesTX    uint64
	MessagesRX uint64
	MessagesTX uint64

	QueuedFrames uint64
	QueuedBytes  uint64

	Backpressure uint64
	DecodeErrors uint64
}

type PacketConnStats struct {
	ResourceID uint64
	Protocol   Scheme
	Local      net.Addr
	Remote     net.Addr
	Age        time.Duration

	BytesRX   uint64
	BytesTX   uint64
	PacketsRX uint64
	PacketsTX uint64

	QueuedPackets uint64
	QueuedBytes   uint64

	Backpressure     uint64
	DroppedDatagrams uint64
}

type ListenerStats struct {
	ResourceID uint64
	Protocol   Scheme
	Local      net.Addr
	Age        time.Duration

	AcceptedConnections uint64
	RejectedConnections uint64
	CurrentConnections  uint64
}
