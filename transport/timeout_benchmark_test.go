package transport

import (
	"testing"
	"time"
)

func BenchmarkActivityClockTouch(b *testing.B) {
	clock := newActivityClock(time.Second, 0)
	if clock == nil {
		b.Fatal("clock = nil")
	}
	b.ReportAllocs()
	for b.Loop() {
		clock.touch()
	}
}

func BenchmarkStreamTimeoutPolicyDisabled(b *testing.B) {
	var clock *activityClock
	b.ReportAllocs()
	for b.Loop() {
		if clock != nil {
			clock.touch()
		}
	}
}
