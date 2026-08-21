package transport

import "github.com/qigao/ogrenet/internal/runtimecore"

type sendGate struct {
	core *runtimecore.SendGate
}

func newSendGate() *sendGate {
	return &sendGate{core: runtimecore.NewSendGate()}
}

func (g *sendGate) enter() bool {
	return g.core.Enter()
}

func (g *sendGate) leave() {
	g.core.Leave()
}

func (g *sendGate) close() <-chan struct{} {
	return g.core.Close()
}

func (g *sendGate) done() <-chan struct{} {
	return g.core.Done()
}
