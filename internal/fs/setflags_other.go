//go:build !linux

package fs

import "os"

func openFile(name string, flag int) (*os.File, error) {
	return os.OpenFile(name, flag, 0)
}
