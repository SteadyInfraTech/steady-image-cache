//go:build !linux

package store

// freeBytes is not implemented on this platform; the free-space check is skipped.
func freeBytes(string) (uint64, bool) { return 0, false }

// totalBytes is not implemented on this platform; percentage reserves are skipped.
func totalBytes(string) (uint64, bool) { return 0, false }
