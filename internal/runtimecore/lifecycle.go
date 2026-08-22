package runtimecore

import "sync"

type CloseGoal uint8

const (
	GoalRunning CloseGoal = iota
	GoalWrite
	GoalFull
	GoalAbort
)

type AbortReason uint8

const (
	AbortNone AbortReason = iota
	AbortExplicit
	AbortCaller
	AbortFailure
)

type Lifecycle struct {
	mu       sync.Mutex
	goal     CloseGoal
	why      AbortReason
	terminal bool

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

func NewLifecycle() *Lifecycle {
	return &Lifecycle{
		writeReq: make(chan struct{}),
		fullReq:  make(chan struct{}),
		abortCh:  make(chan struct{}),
		readCh:   make(chan struct{}),
		writeCh:  make(chan struct{}),
		termCh:   make(chan struct{}),
	}
}

func (l *Lifecycle) Request(goal CloseGoal) bool {
	owner, _ := l.RequestWithPrevious(goal)
	return owner
}

func (l *Lifecycle) RequestWithPrevious(goal CloseGoal) (bool, CloseGoal) {
	if goal <= GoalRunning || goal >= GoalAbort {
		return false, GoalRunning
	}

	l.mu.Lock()
	previous := l.goal
	if l.terminal || l.goal >= goal {
		l.mu.Unlock()
		return false, previous
	}
	l.goal = goal
	l.mu.Unlock()

	l.writeReqOnce.Do(func() { close(l.writeReq) })
	if goal >= GoalFull {
		l.fullReqOnce.Do(func() { close(l.fullReq) })
	}
	return true, previous
}

func (l *Lifecycle) Abort(reason AbortReason) bool {
	return l.AbortWith(reason, nil)
}

func (l *Lifecycle) AbortWith(reason AbortReason, publish func()) bool {
	l.mu.Lock()
	if l.terminal || l.goal == GoalAbort {
		l.mu.Unlock()
		return false
	}
	l.goal = GoalAbort
	l.why = reason
	if publish != nil {
		publish()
	}
	l.mu.Unlock()

	l.writeReqOnce.Do(func() { close(l.writeReq) })
	l.fullReqOnce.Do(func() { close(l.fullReq) })
	l.abortOnce.Do(func() { close(l.abortCh) })
	l.MarkReadClosed()
	l.MarkWriteClosed()
	return true
}

func (l *Lifecycle) Reason() AbortReason {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.why
}

func (l *Lifecycle) WriteRequested() <-chan struct{} { return l.writeReq }
func (l *Lifecycle) FullRequested() <-chan struct{}  { return l.fullReq }
func (l *Lifecycle) Aborted() <-chan struct{}        { return l.abortCh }
func (l *Lifecycle) ReadDone() <-chan struct{}       { return l.readCh }
func (l *Lifecycle) WriteDone() <-chan struct{}      { return l.writeCh }
func (l *Lifecycle) TerminalDone() <-chan struct{}   { return l.termCh }

func (l *Lifecycle) MarkReadClosed() {
	l.readOnce.Do(func() { close(l.readCh) })
}

func (l *Lifecycle) MarkWriteClosed() {
	l.writeOnce.Do(func() { close(l.writeCh) })
}

func (l *Lifecycle) TryMarkTerminal() bool {
	l.mu.Lock()
	if l.terminal || l.goal == GoalAbort {
		l.mu.Unlock()
		return false
	}
	l.terminal = true
	l.mu.Unlock()
	l.markTerminalChannels()
	return true
}

func (l *Lifecycle) MarkTerminal() {
	l.mu.Lock()
	if !l.terminal {
		l.terminal = true
	}
	l.mu.Unlock()
	l.markTerminalChannels()
}

func (l *Lifecycle) markTerminalChannels() {
	l.MarkReadClosed()
	l.MarkWriteClosed()
	l.termOnce.Do(func() { close(l.termCh) })
}
