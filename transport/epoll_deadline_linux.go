//go:build linux

package transport

import (
	"container/heap"
	"time"
)

type epollDeadlineKind uint8

const (
	epollDeadlineConnect epollDeadlineKind = iota + 1
	epollDeadlineWrite
	epollDeadlineReadIdle
	epollDeadlineConnectionIdle
	epollDeadlineMaxLifetime
)

type epollDeadlineTarget interface {
	currentDeadlineGeneration(epollDeadlineKind) uint64
	onReactorDeadline(*epollReactor, epollDeadlineKind, uint64)
}

type epollDeadlineEntry struct {
	at         time.Time
	resourceID uint64
	kind       epollDeadlineKind
	generation uint64
}

type epollDeadlineHeap []epollDeadlineEntry

func (h epollDeadlineHeap) Len() int { return len(h) }

func (h epollDeadlineHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		if h[i].resourceID == h[j].resourceID {
			if h[i].kind == h[j].kind {
				return h[i].generation < h[j].generation
			}
			return h[i].kind < h[j].kind
		}
		return h[i].resourceID < h[j].resourceID
	}
	return h[i].at.Before(h[j].at)
}

func (h epollDeadlineHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *epollDeadlineHeap) Push(value any) {
	*h = append(*h, value.(epollDeadlineEntry))
}

func (h *epollDeadlineHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = epollDeadlineEntry{}
	*h = old[:last]
	return value
}

func (r *epollReactor) scheduleDeadline(resourceID uint64, kind epollDeadlineKind, generation uint64, at time.Time) {
	heap.Push(&r.deadlines, epollDeadlineEntry{
		at:         at,
		resourceID: resourceID,
		kind:       kind,
		generation: generation,
	})
}

func (r *epollReactor) liveDeadlineHead() (epollDeadlineEntry, epollDeadlineTarget, bool) {
	for len(r.deadlines) != 0 {
		entry := r.deadlines[0]
		resource := r.resources[entry.resourceID]
		target, ok := resource.(epollDeadlineTarget)
		if !ok || target == nil || target.currentDeadlineGeneration(entry.kind) != entry.generation {
			heap.Pop(&r.deadlines)
			continue
		}
		return entry, target, true
	}
	return epollDeadlineEntry{}, nil, false
}

func (r *epollReactor) nextWaitTimeout(now time.Time) time.Duration {
	entry, _, ok := r.liveDeadlineHead()
	if !ok {
		return -1
	}
	if !entry.at.After(now) {
		return 0
	}
	return entry.at.Sub(now)
}

func (r *epollReactor) runExpiredDeadlines(now time.Time) {
	for {
		entry, target, ok := r.liveDeadlineHead()
		if !ok || entry.at.After(now) {
			return
		}
		heap.Pop(&r.deadlines)
		target.onReactorDeadline(r, entry.kind, entry.generation)
	}
}
