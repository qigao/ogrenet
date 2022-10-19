package network

type Manager struct {
	num int
	// balance
	polls []*Poller
}

func NewManager(e *Server, number int) (*Manager, error) {
	m := new(Manager)
	m.num = number

	for i := 0; i < number; i++ {
		p, err := NewPoller(e)
		p.index = i
		if err != nil {
			_ = m.Stop()
			return nil, err
		}
		m.polls = append(m.polls, p)
	}

	m.init()
	return m, nil
}

func (m *Manager) init() {
	for _, poller := range m.polls {
		p := poller
		go p.Wait()
	}
}

func (m *Manager) Stop() error {
	for _, poller := range m.polls {
		_ = poller.Close()
	}
	return nil
}

func (m *Manager) Pick(fd int) *Poller {
	return m.polls[fd%m.num]
}
