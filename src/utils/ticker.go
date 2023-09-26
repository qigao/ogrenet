package utils

import "time"

type Ticker struct {
	raw    *time.Ticker
	isDone bool
	done   chan bool
}

func NewTicker(duration time.Duration) *Ticker {
	return &Ticker{
		raw:  time.NewTicker(duration),
		done: make(chan bool),
	}
}

func (t *Ticker) Wait() bool {
	select {
	case <-t.raw.C:
		return true
	case <-t.done:
		t.isDone = true
		return false
	}
}

func (t *Ticker) Stop() {
	if t.isDone {
		return
	}
	t.done <- true
	t.raw.Stop()
}
