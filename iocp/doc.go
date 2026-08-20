// Package iocp provides a small wrapper around Windows I/O completion ports.
//
// It intentionally exposes completion semantics directly. It is not presented
// as an epoll-compatible readiness API because the two kernel mechanisms have
// materially different contracts.
package iocp
