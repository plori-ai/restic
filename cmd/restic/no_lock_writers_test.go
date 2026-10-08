package main

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// lockSaveCounter counts lock files written through the backend.
type lockSaveCounter struct {
	backend.Backend
	saves *atomic.Int64
}

func (b lockSaveCounter) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	if h.Type == backend.LockFile {
		b.saves.Add(1)
	}
	return b.Backend.Save(ctx, h, rd)
}

// backup --no-lock writes the snapshot without writing a lock file; without the
// flag the same backup writes its append lock.
func TestBackupNoLockWritesNoLockFile(t *testing.T) {
	for _, noLock := range []bool{false, true} {
		env, cleanup := withTestEnvironment(t)
		testSetupBackupData(t, env)
		var saves atomic.Int64
		env.gopts.BackendTestHook = func(r backend.Backend) (backend.Backend, error) {
			return lockSaveCounter{Backend: r, saves: &saves}, nil
		}
		gopts := env.gopts
		gopts.NoLock = noLock
		testRunBackup(t, env.testdata, []string{"."}, BackupOptions{}, gopts)
		env.gopts.BackendTestHook = nil
		testListSnapshots(t, env.gopts, 1)
		testRunCheck(t, env.gopts)
		if noLock {
			rtest.Equals(t, int64(0), saves.Load())
		} else {
			rtest.Assert(t, saves.Load() > 0, "backup without --no-lock wrote no lock file")
		}
		cleanup()
	}
}

// A serve-write server started with --no-lock writes under an exclusive lock
// another process holds, and adds no lock file of its own: the caller
// serializes writes with prune.
func TestServeWriteNoLockTakesNoLock(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	f.srv.lockRepo = noRepositoryLock
	exclusive, _, err := repository.Lock(context.TODO(), f.repo, true, 0, func(string) {}, func(string, ...interface{}) {})
	rtest.OK(t, err)
	defer exclusive.Unlock()
	resp := f.edit(base, wr("a/b/file2", "unlocked"))
	_, locked := resp.TimingsMS["lock"]
	rtest.Assert(t, locked, "no lock timing")
	rtest.Equals(t, 1, f.countFiles(restic.LockFile))
}
