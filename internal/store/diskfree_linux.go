//go:build linux

package store

import "syscall"

// freeBytes returns the space available to an unprivileged writer on the
// filesystem holding path. Block counts are in units of the fragment size
// (Frsize), which is not always the same as the preferred I/O size (Bsize).
func freeBytes(path string) (uint64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, false
	}
	unit := uint64(st.Frsize)
	if unit == 0 {
		unit = uint64(st.Bsize)
	}
	return uint64(st.Bavail) * unit, true
}
