//go:build linux

package transport

import "github.com/qigao/ogrenet"

type epollSessionCallbackState uint8

const (
	epollCallbackNeedOpen epollSessionCallbackState = iota + 1
	epollCallbackOpenInFlight
	epollCallbackIdle
	epollCallbackMessageInFlight
	epollCallbackNeedClose
	epollCallbackCloseInFlight
	epollCallbackClosed
)

type epollSessionCallbackTask struct {
	session *epollSession
	state   epollSessionCallbackState
	message ogrenet.Message
}

func (t *epollSessionCallbackTask) runEpollWorkerTask() {
	if t == nil || t.session == nil || t.session.handler == nil {
		return
	}
	switch t.state {
	case epollCallbackOpenInFlight:
		t.session.handler.OnOpen(t.session)
	case epollCallbackMessageInFlight:
		t.session.handler.OnMessage(t.session, t.message)
	case epollCallbackCloseInFlight:
		t.session.handler.OnClose(t.session, t.session.Err())
	}
}

func (t *epollSessionCallbackTask) onEpollWorkerComplete() {
	if t == nil || t.session == nil {
		return
	}
	t.session.publishNativeCallbackCompletion(t.state)
}

func (s *epollSession) initNativeReadCallbacks() {
	if s == nil || s.engine == nil || s.callbackState != 0 {
		return
	}
	s.callbackState = epollCallbackNeedOpen
	readSize := s.engine.cfg.readBuffer
	if readSize <= 0 {
		readSize = defaultReadBuffer
	}
	s.readScratch = make([]byte, readSize)
	s.readPending = make([]byte, 0, readSize)
}

func (s *epollSession) publishNativeCallbackCompletion(state epollSessionCallbackState) {
	s.callbackMu.Lock()
	s.callbackCompleted = state
	s.callbackMu.Unlock()
	if s.reactor != nil {
		s.reactor.signal(s)
	}
}

func (s *epollSession) consumeNativeCallbackCompletion() {
	if s == nil {
		return
	}
	s.callbackMu.Lock()
	completed := s.callbackCompleted
	s.callbackCompleted = 0
	s.callbackMu.Unlock()
	if completed == 0 || completed != s.callbackState {
		return
	}
	switch completed {
	case epollCallbackOpenInFlight:
		s.callbackState = epollCallbackIdle
		if s.state == epollSessionOpening {
			s.state = epollSessionActive
		}
	case epollCallbackMessageInFlight:
		s.callbackState = epollCallbackIdle
	case epollCallbackCloseInFlight:
		s.callbackState = epollCallbackClosed
	}
}

func (s *epollSession) driveNativeCallbackState(r *epollReactor) bool {
	if s == nil || r == nil {
		return false
	}
	for {
		switch s.callbackState {
		case epollCallbackNeedOpen:
			if s.handler == nil {
				s.callbackState = epollCallbackIdle
				if s.state == epollSessionOpening {
					s.state = epollSessionActive
				}
				continue
			}
			if s.engine == nil || s.engine.callbacks == nil || !s.engine.callbacks.tryReserve() {
				r.blockOnWorker(s)
				return false
			}
			s.callbackState = epollCallbackOpenInFlight
			s.engine.callbacks.submitReserved(&epollSessionCallbackTask{session: s, state: epollCallbackOpenInFlight})
			return false
		case epollCallbackOpenInFlight, epollCallbackMessageInFlight, epollCallbackCloseInFlight:
			return false
		case epollCallbackIdle:
			return true
		case epollCallbackNeedClose:
			if s.handler == nil {
				s.callbackState = epollCallbackClosed
				return false
			}
			if s.engine == nil || s.engine.callbacks == nil || !s.engine.callbacks.tryReserve() {
				r.blockOnWorker(s)
				return false
			}
			s.callbackState = epollCallbackCloseInFlight
			s.engine.callbacks.submitReserved(&epollSessionCallbackTask{session: s, state: epollCallbackCloseInFlight})
			return false
		case epollCallbackClosed:
			return false
		default:
			return false
		}
	}
}

func (s *epollSession) driveNativeEstablished(r *epollReactor) {
	if s == nil || r == nil || s.state == epollSessionClosed || s.state == epollSessionTerminal {
		return
	}
	s.driveNativeLifecycle(r)
	if s.state == epollSessionClosed || s.state == epollSessionTerminal {
		return
	}
	if !s.driveNativeCallbackState(r) {
		return
	}
	if s.state == epollSessionActive && s.readReady && !s.nativeReadHalfClosed() {
		s.driveNativeRead(r)
	}
}
