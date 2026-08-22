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
		s.publishCodecSetup(nil, ErrClosed)
		return
	}
	framer, err := s.engine.cfg.newFramer()
	s.publishCodecSetup(framer, err)
}
