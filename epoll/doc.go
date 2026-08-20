// Package epoll provides a small, production-oriented wrapper around Linux epoll.
//
// The package intentionally exposes epoll's readiness model instead of hiding it
// behind a cross-platform abstraction. Callers own the file descriptors and are
// responsible for making them non-blocking when edge-triggered operation is used.
package epoll
