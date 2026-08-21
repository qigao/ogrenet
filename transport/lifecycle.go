package transport

import "sync"

type closeGoal uint8

const (
	closeGoalRunning closeGoal = iota
	closeGoalWrite
	closeGoalFull
	closeGoalAbort
)

type abortReason uint8

const (
	abortNone abortReason = iota
	abortExplicit
	abortCaller
	abortFailure
)

type sessionLifecycle struct {
	mu   sync.Mutex
	goal closeGoal
	why  abortReason

	writeReq chan struct{}
	fullReq  chan struct{}
	abortCh  chan struct{}
	readCh   chan struct{}
	writeCh  chan struct{}
	termCh   chan struct{}

	writeReqOnce sync.Once
	fullReqOnce  sync.Once
	abortOnce    sync.Once
	readOnce     sync.Once
	writeOnce    sync.Once
	termOnce     sync.Once
}

func newSessionLifecycle() *sessionLifecycle {
	return &sessionLifecycle{
		writeReq: make(chan struct{}),
		fullReq:  make(chan struct{}),
		abortCh:  make(chan struct{}),
		readCh:   make(chan struct{}),
		writeCh:  make(chan struct{}),
		termCh:   make(chan struct{}),
	}
}

func (l *sessionLifecycle) request(goal closeGoal) bool {
	if goal <= closeGoalRunning || goal >= closeGoalAbort {
		return false
	}

	l.mu.Lock()
	if l.goal >= goal {
		l.mu.Unlock()
		return false
	}
	l.goal = goal
	l.mu.Unlock()

	l.writeReqOnce.Do(func() { close(l.writeReq) })
	if goal >= closeGoalFull {
		l.fullReqOnce.Do(func() { close(l.fullReq) })
	}
	return true
}

func (l *sessionLifecycle) abort(reason abortReason) bool {
	l.mu.Lock()
	if l.goal == closeGoalAbort {
		l.mu.Unlock()
		return false
	}
	l.goal = closeGoalAbort
	l.why = reason
	l.mu.Unlock()

	l.writeReqOnce.Do(func() { close(l.writeReq) })
	l.fullReqOnce.Do(func() { close(l.fullReq) })
	l.abortOnce.Do(func() { close(l.abortCh) })
	l.markReadClosed()
	l.markWriteClosed()
	return true
}

func (l *sessionLifecycle) reason() abortReason {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.why
}

func (l *sessionLifecycle) writeRequested() <-chan struct{} { return l.writeReq }
func (l *sessionLifecycle) fullRequested() <-chan struct{}  { return l.fullReq }
func (l *sessionLifecycle) aborted() <-chan struct{}        { return l.abortCh }
func (l *sessionLifecycle) readDone() <-chan struct{}       { return l.readCh }
func (l *sessionLifecycle) writeDone() <-chan struct{}      { return l.writeCh }
func (l *sessionLifecycle) terminalDone() <-chan struct{}   { return l.termCh }

func (l *sessionLifecycle) markReadClosed() {
	l.readOnce.Do(func() { close(l.readCh) })
}

func (l *sessionLifecycle) markWriteClosed() {
	l.writeOnce.Do(func() { close(l.writeCh) })
}

func (l *sessionLifecycle) markTerminal() {
	l.markReadClosed()
	l.markWriteClosed()
	l.termOnce.Do(func() { close(l.termCh) })
}
