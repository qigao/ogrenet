# Linux Native Engine 6B Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a real Linux epoll-owned TCP backend behind `transport.NewEpoll`, with fixed reactor ownership, bounded setup/callback execution, non-blocking accept/connect/read/write, graceful half-close, typed-error/Stats/Observer parity, and TCP race/stress/benchmark evidence.

**Architecture:** `transport.New()` remains the portable reference implementation. Linux `NewEpoll` starts N fixed epoll reactors plus one exact-bounded setup/callback executor. Every listener/session fd is assigned to exactly one reactor; only that reactor performs socket creation after assignment, accept/connect progress, read/write, `shutdown(2)`, and terminal close. Application goroutines perform validation, admission, synchronous encoding, ownership publication, and waiting only. Cross-goroutine progress is published through embedded intrusive inbox nodes with coalesced `Poller.Wake()`; work intentionally left ready under EPOLLET is requeued locally rather than waiting for another edge.

**Tech Stack:** Go 1.25+, `golang.org/x/sys/unix`, existing top-level `epoll` poller, root `ogrenet` contracts, existing `transport` admission/quota/error/stats/observer helpers, `internal/runtimecore`, race detector, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-22-linux-native-engine-design.md`
