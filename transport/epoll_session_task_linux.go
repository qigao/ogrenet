//go:build linux

package transport

type epollCodecSetupTask struct {
	session *epollSession
}

func (t *epollCodecSetupTask) runEpollWorkerTask() {
	if t == nil || t.session == nil {
		return
	}
	s := t.session
	if s.engine == nil {
		s.storeCodecSetup(nil, ErrClosed)
		return
	}
	framer, err := s.engine.cfg.newFramer()
	s.storeCodecSetup(framer, err)
}

func (t *epollCodecSetupTask) onEpollWorkerComplete() {
	if t == nil || t.session == nil {
		return
	}
	t.session.notifyCodecSetup()
}
