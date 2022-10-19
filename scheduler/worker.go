package scheduler

// Worker represents a working goroutine.
type Worker struct {
	pool *Pool
	task chan Task
}

// StartWorker create a new worker.
func StartWorker(pool *Pool) {
	worker := &Worker{
		pool: pool,
		task: make(chan Task),
	}

	go worker.work()
}

// Worker's main loop.
func (w *Worker) work() {
	wrapper := func(task Task) {
		defer func() {
			if r := recover(); r != nil {
				return
			}
		}()

		task.Do()
	}

	w.pool.workers <- w.task

	for {
		select {
		case t := <-w.task:
			wrapper(t)
			w.pool.workers <- w.task
		}
	}
}
