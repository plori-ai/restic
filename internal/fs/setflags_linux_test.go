package fs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	rtest "github.com/restic/restic/internal/test"

	"golang.org/x/sys/unix"
)

func TestNoatime(t *testing.T) {
	f, err := os.CreateTemp("", "restic-test-noatime")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = f.Close()
		err = os.Remove(f.Name())
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Only run this test on common filesystems that support O_NOATIME.
	// On others, we may not get an error.
	if !supportsNoatime(t, f) {
		t.Skip("temp directory may not support O_NOATIME, skipping")
	}
	// From this point on, we own the file, so we should not get EPERM.

	_, err = io.WriteString(f, "Hello!")
	rtest.OK(t, err)
	_, err = f.Seek(0, io.SeekStart)
	rtest.OK(t, err)

	getAtime := func() time.Time {
		info, err := f.Stat()
		rtest.OK(t, err)
		return ExtendedStat(info).AccessTime
	}

	atime := getAtime()

	reader, err := openFile(f.Name(), os.O_RDONLY)
	rtest.OK(t, err)
	defer func() { rtest.OK(t, reader.Close()) }()
	flags, err := unix.FcntlInt(reader.Fd(), unix.F_GETFL, 0)
	rtest.OK(t, err)
	rtest.Assert(t, flags&unix.O_NOATIME != 0, "O_NOATIME was not set")

	_, err = reader.Read(make([]byte, 1))
	rtest.OK(t, err)
	rtest.Equals(t, atime, getAtime())
}

func supportsNoatime(t *testing.T, f *os.File) bool {
	var fsinfo unix.Statfs_t
	err := unix.Fstatfs(int(f.Fd()), &fsinfo)
	rtest.OK(t, err)

	// The funky cast works around a compiler error on 32-bit archs:
	// "unix.BTRFS_SUPER_MAGIC (untyped int constant 2435016766) overflows int32".
	// https://github.com/golang/go/issues/52061
	typ := int64(uint(fsinfo.Type))
	return typ == unix.BTRFS_SUPER_MAGIC ||
		typ == unix.EXT2_SUPER_MAGIC ||
		typ == unix.EXT3_SUPER_MAGIC ||
		typ == unix.EXT4_SUPER_MAGIC ||
		typ == unix.TMPFS_MAGIC
}

func TestOpenFileNoatimeFallback(t *testing.T) {
	// /proc files are owned by root and may reject O_NOATIME. Test the real
	// permission fallback when running as an ordinary user without CAP_FOWNER.
	name := "/proc/sys/kernel/hostname"
	f, err := os.OpenFile(name, os.O_RDONLY|unix.O_NOATIME, 0)
	if err == nil {
		rtest.OK(t, f.Close())
		t.Skip("O_NOATIME permitted; fallback requires an unprivileged user")
	}
	if !errors.Is(err, unix.EPERM) {
		t.Skipf("cannot test permission fallback: %v", err)
	}
	f, err = openFile(name, os.O_RDONLY)
	rtest.OK(t, err)
	defer func() { rtest.OK(t, f.Close()) }()
	_, err = io.ReadAll(f)
	rtest.OK(t, err)
}

func TestOpenFileNoatimeErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	_, err := openFile(path, os.O_RDONLY)
	rtest.Assert(t, os.IsNotExist(err), "missing file error lost: %v", err)
	rtest.OK(t, os.WriteFile(path, nil, 0o600))
	link := path + "-link"
	rtest.OK(t, os.Symlink(path, link))
	_, err = openFile(link, os.O_RDONLY|O_NOFOLLOW)
	rtest.Assert(t, errors.Is(err, unix.ELOOP), "O_NOFOLLOW was not preserved: %v", err)
	_, err = openFile(path, os.O_RDONLY|O_DIRECTORY)
	rtest.Assert(t, errors.Is(err, unix.ENOTDIR), "O_DIRECTORY was not preserved: %v", err)
}
