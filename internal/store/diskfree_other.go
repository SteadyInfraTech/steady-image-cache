//go:build !linux

package store

// freeBytes is not implemented on this platform; the free-space check is skipped.
func freeBytes(string) (uint64, bool) { return 0, false }
