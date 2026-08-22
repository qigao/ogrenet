# Epoll UDP race model

This Lean package models three concurrency failures that have direct counterparts in the Linux native UDP implementation:

1. a readable EPOLLET edge delivered before the PacketConn becomes Active can be consumed without retaining local readiness;
2. a stale runtime deadline can close a resource after newer network progress unless generation ownership rejects it;
3. a later timeout publisher can overwrite an earlier explicit Close unless terminal ownership is first-writer-wins.

`EpollUdpRace.lean` contains both an unsafe transition system with explicit reachable counterexamples and the corresponding safe transition rules. The safe rules model the implementation contracts used by the reactor: pre-active readiness preservation, generation-checked deadlines, and first-terminal-owner arbitration.

The proof package is intentionally independent of the Go runtime. A failed theorem means the abstract ownership/state contract is inconsistent before any stochastic Go race test is considered.
