package scheduler

import (
	"errors"
	"runtime"
	"time"
)

// ErrScheduleTimeout happens when task schedule failed during the specific interval.
var ErrScheduleTimeout = errors.New("schedule not available currently")

// Pool caches tasks and schedule tasks to work.
type Pool struct {
	queue   chan Task
	workers chan chan Task
}

// New a goroutine pool.
func New(qsize, wsize int) *Pool {
	if wsize == 0 {
		wsize = runtime.NumCPU()
	}

	if qsize < wsize {
		qsize = wsize
	}

	pool := &Pool{
		queue:   make(chan Task, qsize),
		workers: make(chan chan Task, wsize),
	}

	go pool.start()

	for i := 0; i < wsize; i++ {
		StartWorker(pool)
	}

	return pool
}

// Starts the scheduling.
func (p *Pool) start() {
	for {
		select {
		case worker := <-p.workers:
			task := <-p.queue
			worker <- task
		}
	}
}

// Schedule push a task on queue.
func (p *Pool) Schedule(task Task) {
	p.queue <- task
}

// ScheduleWithTimeout try to push a task on queue, if timeout, return false.
func (p *Pool) ScheduleWithTimeout(timeout time.Duration, task Task) error {
	timer := time.NewTimer(timeout)

	select {
	case p.queue <- task:
		timer.Stop()
		return nil
	case <-timer.C:
		return ErrScheduleTimeout
	}
}
