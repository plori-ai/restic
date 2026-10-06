package fs

import (
	"os"

	"golang.org/x/sys/unix"
)

// openFile tries O_NOATIME at open time to avoid two fcntl calls per file.
//
// If the flag is unsupported or we're not the owner of the file or root,
// retry without it, just as the previous best-effort F_SETFL did.
func openFile(name string, flag int) (*os.File, error) {
	f, err := os.OpenFile(name, flag|unix.O_NOATIME, 0)
	if err != nil {
		return os.OpenFile(name, flag, 0)
	}
	return f, nil
}
