//go:build linux

package transport

// epollListener is completed by the native listen/accept step. The bootstrap
// declaration lives here so epollSession can retain its parent identity while
// codec setup is developed independently of listener socket ownership.
type epollListener struct {
	id uint64
}
