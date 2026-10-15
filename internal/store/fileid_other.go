//go:build !unix

package store

import "os"

// inode is not available on this platform; verification then relies on size and
// modification time alone.
func inode(os.FileInfo) uint64 { return 0 }
