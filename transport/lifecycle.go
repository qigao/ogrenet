package transport

import "github.com/qigao/ogrenet/internal/runtimecore"

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
	core *runtimecore.Lifecycle
}

func newSessionLifecycle() *sessionLifecycle {
	return &sessionLifecycle{core: runtimecore.NewLifecycle()}
}

func (l *sessionLifecycle) request(goal closeGoal) bool {
	return l.core.Request(toCoreCloseGoal(goal))
}

func (l *sessionLifecycle) requestWithPrevious(goal closeGoal) (bool, closeGoal) {
	owner, previous := l.core.RequestWithPrevious(toCoreCloseGoal(goal))
	return owner, fromCoreCloseGoal(previous)
}

func (l *sessionLifecycle) abort(reason abortReason) bool {
	return l.core.Abort(toCoreAbortReason(reason))
}

// abortWith lets the winning abort publish resource terminal state while
// ownership is still serialized, before any abort/read/write completion signal
// becomes observable. Losing aborts cannot return until that publication has
// completed.
func (l *sessionLifecycle) abortWith(reason abortReason, publish func()) bool {
	return l.core.AbortWith(toCoreAbortReason(reason), publish)
}

func (l *sessionLifecycle) reason() abortReason {
	return fromCoreAbortReason(l.core.Reason())
}

func (l *sessionLifecycle) writeRequested() <-chan struct{} { return l.core.WriteRequested() }
func (l *sessionLifecycle) fullRequested() <-chan struct{}  { return l.core.FullRequested() }
func (l *sessionLifecycle) aborted() <-chan struct{}        { return l.core.Aborted() }
func (l *sessionLifecycle) readDone() <-chan struct{}       { return l.core.ReadDone() }
func (l *sessionLifecycle) writeDone() <-chan struct{}      { return l.core.WriteDone() }
func (l *sessionLifecycle) terminalDone() <-chan struct{}   { return l.core.TerminalDone() }

func (l *sessionLifecycle) markReadClosed()  { l.core.MarkReadClosed() }
func (l *sessionLifecycle) markWriteClosed() { l.core.MarkWriteClosed() }
func (l *sessionLifecycle) tryMarkTerminal() bool {
	return l.core.TryMarkTerminal()
}
func (l *sessionLifecycle) markTerminal() { l.core.MarkTerminal() }

func toCoreCloseGoal(goal closeGoal) runtimecore.CloseGoal {
	switch goal {
	case closeGoalWrite:
		return runtimecore.GoalWrite
	case closeGoalFull:
		return runtimecore.GoalFull
	case closeGoalAbort:
		return runtimecore.GoalAbort
	default:
		return runtimecore.GoalRunning
	}
}

func fromCoreCloseGoal(goal runtimecore.CloseGoal) closeGoal {
	switch goal {
	case runtimecore.GoalWrite:
		return closeGoalWrite
	case runtimecore.GoalFull:
		return closeGoalFull
	case runtimecore.GoalAbort:
		return closeGoalAbort
	default:
		return closeGoalRunning
	}
}

func toCoreAbortReason(reason abortReason) runtimecore.AbortReason {
	switch reason {
	case abortExplicit:
		return runtimecore.AbortExplicit
	case abortCaller:
		return runtimecore.AbortCaller
	case abortFailure:
		return runtimecore.AbortFailure
	default:
		return runtimecore.AbortNone
	}
}

func fromCoreAbortReason(reason runtimecore.AbortReason) abortReason {
	switch reason {
	case runtimecore.AbortExplicit:
		return abortExplicit
	case runtimecore.AbortCaller:
		return abortCaller
	case runtimecore.AbortFailure:
		return abortFailure
	default:
		return abortNone
	}
}
