package transport

import "context"

func (s *wsSession) operationalError(op Op, cause error, hint classifyHint) error {
	return classifyOperational(op, s.protocol, s.local, s.remote, cause, hint)
}

func (s *wsSession) sendError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
	}
	return s.operationalError(OpSend, err, hintNone)
}
