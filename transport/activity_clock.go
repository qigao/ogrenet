package transport

import (
	"sync/atomic"
	"time"
)

type activityClock struct {
	born           time.Time
	lastActivityNS atomic.Int64
	wake           chan struct{}
	connectionIdle time.Duration
	maxLifetime    time.Duration
}

func newActivityClock(connectionIdle, maxLifetime time.Duration) *activityClock {
	if connectionIdle <= 0 && maxLifetime <= 0 {
		return nil
	}
	return &activityClock{
		born:           time.Now(),
		wake:           make(chan struct{}, 1),
		connectionIdle: connectionIdle,
		maxLifetime:    maxLifetime,
	}
}

func (c *activityClock) touch() {
	if c == nil {
		return
	}
	elapsed := time.Since(c.born)
	c.lastActivityNS.Store(elapsed.Nanoseconds())
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *activityClock) nextDeadline() (time.Time, TimeoutKind) {
	if c == nil {
		return time.Time{}, 0
	}
	var (
		deadline time.Time
		kind     TimeoutKind
	)
	if c.connectionIdle > 0 {
		last := time.Duration(c.lastActivityNS.Load())
		deadline = c.born.Add(last + c.connectionIdle)
		kind = TimeoutConnectionIdle
	}
	if c.maxLifetime > 0 {
		lifetime := c.born.Add(c.maxLifetime)
		if deadline.IsZero() || !lifetime.After(deadline) {
			deadline = lifetime
			kind = TimeoutMaxLifetime
		}
	}
	return deadline, kind
}

func (c *activityClock) run(closing <-chan struct{}, expire func(TimeoutKind)) {
	if c == nil {
		return
	}
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		deadline, kind := c.nextDeadline()
		if kind == 0 {
			return
		}
		delay := time.Until(deadline)
		if delay < 0 {
			delay = 0
		}
		resetActivityTimer(timer, delay)

		select {
		case <-closing:
			return
		case <-c.wake:
			continue
		case <-timer.C:
			currentDeadline, currentKind := c.nextDeadline()
			if !currentDeadline.After(time.Now()) {
				if expire != nil {
					expire(currentKind)
				}
				return
			}
		}
	}
}

func resetActivityTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}
