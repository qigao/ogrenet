//go:build linux

package transport

import (
	"context"
	"testing"
	"time"
)

type blockingWorkerTask struct {
	entered chan struct{}
	release <-chan struct{}
	done    chan struct{}
}

func newBlockingWorkerTask(release <-chan struct{}) *blockingWorkerTask {
	return &blockingWorkerTask{
		entered: make(chan struct{}),
		release: release,
		done:    make(chan struct{}),
	}
}

func (t *blockingWorkerTask) runEpollWorkerTask() {
	close(t.entered)
	<-t.release
	close(t.done)
}

func waitCallbackTestSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", what, context.Cause(ctx))
	}
}

func TestEpollCallbackExecutorReservationBound(t *testing.T) {
	x := newEpollCallbackExecutor(1, 1, nil)
	if !x.tryReserve() || !x.tryReserve() {
		t.Fatal("two reservations should fit workers+queue bound")
	}
	if x.tryReserve() {
		t.Fatal("third reservation exceeded workers+queue bound")
	}
	if got := x.reservedCount(); got != 2 {
		t.Fatalf("reserved=%d, want 2", got)
	}
	x.releaseReserved()
	x.releaseReserved()
	x.stopIdle()
}

func TestEpollCallbackExecutorQueueNeverExceedsConfiguredQueue(t *testing.T) {
	x := newEpollCallbackExecutor(1, 1, nil)
	release1 := make(chan struct{})
	release2 := make(chan struct{})
	task1 := newBlockingWorkerTask(release1)
	task2 := newBlockingWorkerTask(release2)

	if !x.tryReserve() || !x.tryReserve() {
		t.Fatal("failed to reserve running and queued tasks")
	}
	x.submitReserved(task1)
	waitCallbackTestSignal(t, task1.entered, "first worker task")
	x.submitReserved(task2)
	if got := x.queuedCount(); got != 1 {
		t.Fatalf("queued=%d, want 1", got)
	}
	if got := x.reservedCount(); got != 2 {
		t.Fatalf("reserved=%d, want 2", got)
	}
	if x.tryReserve() {
		t.Fatal("reservation succeeded with worker and queue both occupied")
	}

	close(release1)
	waitCallbackTestSignal(t, task1.done, "first worker completion")
	waitCallbackTestSignal(t, task2.entered, "second worker task")
	close(release2)
	waitCallbackTestSignal(t, task2.done, "second worker completion")
	waitCallbackExecutorIdle(t, x)
	x.stopIdle()
}

func TestEpollCallbackExecutorBlockedTaskDoesNotBlockSubmitter(t *testing.T) {
	x := newEpollCallbackExecutor(1, 1, nil)
	release1 := make(chan struct{})
	release2 := make(chan struct{})
	task1 := newBlockingWorkerTask(release1)
	task2 := newBlockingWorkerTask(release2)

	if !x.tryReserve() || !x.tryReserve() {
		t.Fatal("failed to reserve two tasks")
	}
	x.submitReserved(task1)
	waitCallbackTestSignal(t, task1.entered, "blocked first task")

	submitted := make(chan struct{})
	go func() {
		x.submitReserved(task2)
		close(submitted)
	}()
	waitCallbackTestSignal(t, submitted, "nonblocking second submit")
	if got := x.queuedCount(); got != 1 {
		t.Fatalf("queued=%d, want 1 while first task is blocked", got)
	}

	close(release1)
	waitCallbackTestSignal(t, task2.entered, "second task start")
	close(release2)
	waitCallbackTestSignal(t, task2.done, "second task completion")
	waitCallbackExecutorIdle(t, x)
	x.stopIdle()
}

func TestEpollCallbackExecutorCapacityReleaseNotifies(t *testing.T) {
	capacity := make(chan struct{}, 4)
	x := newEpollCallbackExecutor(1, 1, func() {
		select {
		case capacity <- struct{}{}:
		default:
		}
	})

	if !x.tryReserve() {
		t.Fatal("reservation failed")
	}
	x.releaseReserved()
	waitCallbackTestSignal(t, capacity, "released unused reservation notification")
	if got := x.reservedCount(); got != 0 {
		t.Fatalf("reserved=%d after release, want 0", got)
	}

	release := make(chan struct{})
	task := newBlockingWorkerTask(release)
	if !x.tryReserve() {
		t.Fatal("task reservation failed")
	}
	x.submitReserved(task)
	waitCallbackTestSignal(t, task.entered, "capacity task start")
	close(release)
	waitCallbackTestSignal(t, task.done, "capacity task completion")
	waitCallbackTestSignal(t, capacity, "completed task capacity notification")
	waitCallbackExecutorIdle(t, x)
	x.stopIdle()
}

func TestEpollCallbackExecutorStopsOnlyWhenIdle(t *testing.T) {
	x := newEpollCallbackExecutor(1, 1, nil)
	if !x.tryReserve() {
		t.Fatal("reservation failed")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("stopIdle did not reject outstanding reservation")
			}
		}()
		x.stopIdle()
	}()
	x.releaseReserved()
	x.stopIdle()
	if x.tryReserve() {
		t.Fatal("stopped executor accepted a reservation")
	}
}

func waitCallbackExecutorIdle(t *testing.T, x *epollCallbackExecutor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		if x.reservedCount() == 0 && x.queuedCount() == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("executor did not become idle: reserved=%d queued=%d", x.reservedCount(), x.queuedCount())
		default:
		}
	}
}
