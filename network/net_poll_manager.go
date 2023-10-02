package network

import (
	"sync"

	"golang.org/x/exp/maps"
)

type NetPollManager struct {
	num   int
	mutex sync.RWMutex
	polls map[int]*NetPoll
}

func NewNetPollManager(ogreNet *OgreNet, number int) (*NetPollManager, error) {
	m := new(NetPollManager)
	m.num = number
	m.mutex = sync.RWMutex{}
	m.polls = make(map[int]*NetPoll)
	for i := 0; i < number; i++ {
		p, err := NewNetPoll(ogreNet)
		p.index = i
		if err != nil {
			_ = m.Stop()
			return nil, err
		}
		m.polls[i] = p
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
	m.mutex.Lock()
	for _, poller := range m.polls {
		_ = poller.Close()
	}
	maps.Clear(m.polls)
	m.mutex.Unlock()
	m.num = 0
	return nil
}

func (m *NetPollManager) Pick(fd int) *NetPoll {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.polls[fd%m.num]
}
