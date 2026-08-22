//go:build linux

package transport

import "sync"

type epollWorkerTask interface {
	runEpollWorkerTask()
}

type epollCallbackWorker struct {
	tasks chan epollWorkerTask
}

type epollCallbackExecutor struct {
	mu sync.Mutex

	workers []*epollCallbackWorker
	idle    []*epollCallbackWorker

	queue []epollWorkerTask
	head  int
	size  int

	reserved int
	limit    int
	stopped  bool

	onCapacity func()
	wg         sync.WaitGroup
}

func newEpollCallbackExecutor(workers, queue int, onCapacity func()) *epollCallbackExecutor {
	if workers <= 0 {
		panic("transport: epoll callback executor requires at least one worker")
	}
	if queue < 0 {
		panic("transport: epoll callback executor queue must be non-negative")
	}
	x := &epollCallbackExecutor{
		workers:    make([]*epollCallbackWorker, 0, workers),
		idle:       make([]*epollCallbackWorker, 0, workers),
		queue:      make([]epollWorkerTask, queue),
		limit:      workers + queue,
		onCapacity: onCapacity,
	}
	for i := 0; i < workers; i++ {
		w := &epollCallbackWorker{tasks: make(chan epollWorkerTask, 1)}
		x.workers = append(x.workers, w)
		x.idle = append(x.idle, w)
		x.wg.Add(1)
		go x.runWorker(w)
	}
	return x
}

func (x *epollCallbackExecutor) runWorker(w *epollCallbackWorker) {
	defer x.wg.Done()
	for task := range w.tasks {
		task.runEpollWorkerTask()
		x.completeTask(w)
	}
}

func (x *epollCallbackExecutor) tryReserve() bool {
	if x == nil {
		return false
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.stopped || x.reserved >= x.limit {
		return false
	}
	x.reserved++
	return true
}

func (x *epollCallbackExecutor) hasCapacity() bool {
	if x == nil {
		return false
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	return !x.stopped && x.reserved < x.limit
}

func (x *epollCallbackExecutor) submitReserved(task epollWorkerTask) {
	if x == nil || task == nil {
		panic("transport: invalid epoll callback task")
	}

	x.mu.Lock()
	if x.stopped {
		x.mu.Unlock()
		panic("transport: submit to stopped epoll callback executor")
	}
	running := len(x.workers) - len(x.idle)
	if x.reserved <= running+x.size {
		x.mu.Unlock()
		panic("transport: epoll callback submit without reservation")
	}

	if len(x.idle) != 0 {
		last := len(x.idle) - 1
		worker := x.idle[last]
		x.idle[last] = nil
		x.idle = x.idle[:last]
		x.mu.Unlock()
		worker.tasks <- task
		return
	}

	if x.size >= len(x.queue) {
		x.mu.Unlock()
		panic("transport: epoll callback queue exceeded reserved capacity")
	}
	index := (x.head + x.size) % len(x.queue)
	x.queue[index] = task
	x.size++
	x.mu.Unlock()
}

func (x *epollCallbackExecutor) completeTask(worker *epollCallbackWorker) {
	var next epollWorkerTask
	x.mu.Lock()
	if x.reserved <= 0 {
		x.mu.Unlock()
		panic("transport: epoll callback reservation underflow")
	}
	x.reserved--
	if x.size != 0 {
		next = x.queue[x.head]
		x.queue[x.head] = nil
		x.head++
		if x.head == len(x.queue) {
			x.head = 0
		}
		x.size--
	} else {
		x.idle = append(x.idle, worker)
	}
	onCapacity := x.onCapacity
	x.mu.Unlock()

	if next != nil {
		worker.tasks <- next
	}
	if onCapacity != nil {
		onCapacity()
	}
}

func (x *epollCallbackExecutor) releaseReserved() {
	if x == nil {
		panic("transport: release reservation on nil epoll callback executor")
	}
	x.mu.Lock()
	running := len(x.workers) - len(x.idle)
	unsubmitted := x.reserved - running - x.size
	if unsubmitted <= 0 {
		x.mu.Unlock()
		panic("transport: release missing epoll callback reservation")
	}
	x.reserved--
	onCapacity := x.onCapacity
	x.mu.Unlock()
	if onCapacity != nil {
		onCapacity()
	}
}

func (x *epollCallbackExecutor) reservedCount() int {
	if x == nil {
		return 0
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.reserved
}

func (x *epollCallbackExecutor) queuedCount() int {
	if x == nil {
		return 0
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.size
}

func (x *epollCallbackExecutor) stopIdle() {
	if x == nil {
		return
	}
	x.mu.Lock()
	if x.stopped {
		x.mu.Unlock()
		x.wg.Wait()
		return
	}
	if x.reserved != 0 || x.size != 0 || len(x.idle) != len(x.workers) {
		x.mu.Unlock()
		panic("transport: stop non-idle epoll callback executor")
	}
	x.stopped = true
	workers := append([]*epollCallbackWorker(nil), x.workers...)
	x.mu.Unlock()

	for _, worker := range workers {
		close(worker.tasks)
	}
	x.wg.Wait()
}
