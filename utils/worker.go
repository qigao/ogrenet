package utils

import (
	"time"
)

type Worker struct {
	id       string
	ch       chan func()
	isClosed bool
}

func NewWorker(id string) *Worker {
	worker := &Worker{
		id: id,
		ch: make(chan func(), 128),
	}
	worker.setup()
	return worker
}

func (w *Worker) Run(task func()) {
	defer func() {
		recover()
	}()

	// 此处可能造成阻塞，但保证了任务是同步执行的
	w.ch <- task
}

func (w *Worker) Id() string {
	return w.id
}

func (w *Worker) Close() {
	if w.isClosed {
		return
	}
	w.isClosed = true

	// 延时N秒再关闭
	timeout := time.NewTimer(5 * time.Second)
	go func() {
		defer func() {
			recover()
		}()

		select {
		case <-timeout.C:
			close(w.ch)
		}
	}()
}

func (w *Worker) setup() {
	go func() {
		for task := range w.ch {
			if task == nil {
				break
			}
			task()
		}
	}()
}
