package transport

import "math"

const (
	defaultEpollEventBatch       = 256
	defaultEpollCallbackQueue    = 64
	defaultEpollMaxCallbackQueue = 1024
	defaultEpollIOBudgetBytes    = 256 << 10
	defaultEpollIOBudgetOps      = 64
)

type EpollConfig struct {
	Pollers         int
	EventBatch      int
	CallbackWorkers int
	CallbackQueue   int
	IOBudgetBytes   int
	IOBudgetOps     int
}

type resolvedEpollConfig struct {
	pollers         int
	eventBatch      int
	callbackWorkers int
	callbackQueue   int
	ioBudgetBytes   int
	ioBudgetOps     int
}

func resolveEpollConfig(cfg EpollConfig, gomaxprocs int) (resolvedEpollConfig, error) {
	if cfg.Pollers < 0 ||
		cfg.EventBatch < 0 ||
		cfg.CallbackWorkers < 0 ||
		cfg.CallbackQueue < 0 ||
		cfg.IOBudgetBytes < 0 ||
		cfg.IOBudgetOps < 0 {
		return resolvedEpollConfig{}, ErrInvalidEpollConfig
	}
	if gomaxprocs < 1 {
		gomaxprocs = 1
	}

	r := resolvedEpollConfig{
		pollers:         cfg.Pollers,
		eventBatch:      cfg.EventBatch,
		callbackWorkers: cfg.CallbackWorkers,
		callbackQueue:   cfg.CallbackQueue,
		ioBudgetBytes:   cfg.IOBudgetBytes,
		ioBudgetOps:     cfg.IOBudgetOps,
	}
	if r.pollers == 0 {
		r.pollers = gomaxprocs
	}
	if r.eventBatch == 0 {
		r.eventBatch = defaultEpollEventBatch
	}
	if r.callbackWorkers == 0 {
		r.callbackWorkers = gomaxprocs
	}
	if r.callbackQueue == 0 {
		if r.callbackWorkers > math.MaxInt/4 {
			return resolvedEpollConfig{}, ErrInvalidEpollConfig
		}
		r.callbackQueue = 4 * r.callbackWorkers
		if r.callbackQueue < defaultEpollCallbackQueue {
			r.callbackQueue = defaultEpollCallbackQueue
		}
		if r.callbackQueue > defaultEpollMaxCallbackQueue {
			r.callbackQueue = defaultEpollMaxCallbackQueue
		}
	}
	if r.ioBudgetBytes == 0 {
		r.ioBudgetBytes = defaultEpollIOBudgetBytes
	}
	if r.ioBudgetOps == 0 {
		r.ioBudgetOps = defaultEpollIOBudgetOps
	}
	return r, nil
}
