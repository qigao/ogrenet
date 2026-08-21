package transport

func (p *packetConn) requestDrain() {
	p.drainOnce.Do(func() {
		p.gate.close()
		close(p.drainReq)
	})
}
