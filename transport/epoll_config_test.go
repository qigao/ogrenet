package transport

import (
	"errors"
	"math"
	"testing"
)

func TestResolveEpollConfigDefaults(t *testing.T) {
	got, err := resolveEpollConfig(EpollConfig{}, 8)
	if err != nil {
		t.Fatal(err)
	}
	want := resolvedEpollConfig{
		pollers:         8,
		eventBatch:      256,
		callbackWorkers: 8,
		callbackQueue:   64,
		ioBudgetBytes:   256 << 10,
		ioBudgetOps:     64,
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestResolveEpollConfigExplicitValues(t *testing.T) {
	cfg := EpollConfig{
		Pollers:         2,
		EventBatch:      33,
		CallbackWorkers: 3,
		CallbackQueue:   7,
		IOBudgetBytes:   4096,
		IOBudgetOps:     9,
	}
	got, err := resolveEpollConfig(cfg, 99)
	if err != nil {
		t.Fatal(err)
	}
	want := resolvedEpollConfig{
		pollers:         2,
		eventBatch:      33,
		callbackWorkers: 3,
		callbackQueue:   7,
		ioBudgetBytes:   4096,
		ioBudgetOps:     9,
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestResolveEpollConfigRejectsInvalidValues(t *testing.T) {
	cases := []EpollConfig{
		{Pollers: -1},
		{EventBatch: -1},
		{CallbackWorkers: -1},
		{CallbackQueue: -1},
		{IOBudgetBytes: -1},
		{IOBudgetOps: -1},
		{CallbackWorkers: math.MaxInt},
	}
	for _, cfg := range cases {
		if _, err := resolveEpollConfig(cfg, 4); !errors.Is(err, ErrInvalidEpollConfig) {
			t.Fatalf("cfg=%+v err=%v", cfg, err)
		}
	}
}

func TestResolveEpollConfigNormalizesNonPositiveGOMAXPROCS(t *testing.T) {
	got, err := resolveEpollConfig(EpollConfig{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.pollers != 1 || got.callbackWorkers != 1 {
		t.Fatalf("got pollers=%d callbackWorkers=%d", got.pollers, got.callbackWorkers)
	}
}
