//go:build unix

package store

import (
	"os"
	"syscall"
)

// inode returns the file's inode number, or 0 where the platform has none.
func inode(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}
