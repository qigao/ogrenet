package network

type NetPollManager struct {
	num int
	// balance
	polls []*NetPoll
}

func NewNetPollManager(ogreNet *OgreNet, number int) (*NetPollManager, error) {
	m := new(NetPollManager)
	m.num = number

	for i := 0; i < number; i++ {
		p, err := NewNetPoll(ogreNet)
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

func (m *NetPollManager) init() {
	for _, poller := range m.polls {
		p := poller
		go p.Wait()
	}
}

func (m *NetPollManager) Stop() error {
	for _, poller := range m.polls {
		_ = poller.Close()
	}
	return nil
}

func (m *NetPollManager) Pick(fd int) *NetPoll {
	return m.polls[fd%m.num]
}
