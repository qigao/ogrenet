// Package kqueue exposes a thin kqueue wrapper for supported 64-bit Darwin and FreeBSD targets.
//
// It preserves kqueue's native filter/change model instead of translating it
// into epoll-style readiness flags.
package kqueue
