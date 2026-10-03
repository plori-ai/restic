//go:build linux

package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/xattr"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func TestBackupTreeMetadataParent(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	testRunInit(t, env.gopts)
	src := env.testdata
	rtest.OK(t, os.Mkdir(filepath.Join(src, "sub"), 0750))
	for _, p := range []string{"a", "c", "single"} {
		rtest.OK(t, os.WriteFile(filepath.Join(src, p), []byte("data"), 0640))
		rtest.OK(t, os.Chmod(filepath.Join(src, p), 0640))
	}
	rtest.OK(t, os.Link(filepath.Join(src, "a"), filepath.Join(src, "sub", "b")))
	rtest.OK(t, os.Link(filepath.Join(src, "c"), filepath.Join(src, "sub", "d")))
	rtest.OK(t, os.Symlink("a", filepath.Join(src, "symlink")))
	rtest.OK(t, xattr.Set(filepath.Join(src, "single"), "user.stabletree", []byte("original")))
	mtime := time.Unix(1700000000, 123456789)
	for _, p := range []string{"a", "c", "single", "sub"} {
		rtest.OK(t, os.Chtimes(filepath.Join(src, p), mtime, mtime))
	}
	testRunBackup(t, src, []string{"."}, BackupOptions{}, env.gopts)
	seed, _ := testRunSnapshots(t, env.gopts)
	seedID := seed.ID.String()
	head := testLoadSnapshot(t, env.gopts, *seed.ID)

	backup := func(t *testing.T, dir string, opts BackupOptions) *data.Snapshot {
		t.Helper()
		before := loadSnapshotMap(t, env.gopts)
		testRunBackup(t, dir, []string{"."}, opts, env.gopts)
		_, id := lastSnapshot(before, loadSnapshotMap(t, env.gopts))
		rtest.Assert(t, id != "", "expected new snapshot")
		parsed, err := restic.ParseID(id)
		rtest.OK(t, err)
		return testLoadSnapshot(t, env.gopts, parsed)
	}
	restore := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		testRunRestore(t, env.gopts, dir, seedID)
		return dir
	}
	assertGroups := func(t *testing.T, dir string, joined, split bool) {
		t.Helper()
		stat := func(p string) os.FileInfo {
			fi, err := os.Stat(filepath.Join(dir, p))
			rtest.OK(t, err)
			return fi
		}
		rtest.Equals(t, !split, os.SameFile(stat("a"), stat("sub/b")))
		rtest.Assert(t, os.SameFile(stat("c"), stat("sub/d")), "second group split")
		rtest.Equals(t, joined, os.SameFile(stat("a"), stat("c")))
		rtest.Assert(t, !os.SameFile(stat("a"), stat("single")), "single file merged")
	}
	opts := BackupOptions{Parent: seedID, TreeMetadataParent: seedID}
	for _, label := range []string{"fresh-A", "fresh-B"} {
		t.Run(label, func(t *testing.T) {
			dir := restore(t)
			assertGroups(t, dir, false, false)
			sn := backup(t, dir, opts)
			rtest.Equals(t, head.Tree, sn.Tree)
			rtest.Equals(t, 0, sn.Summary.TreeBlobs)
			rtest.Equals(t, 0, sn.Summary.DataBlobs)
			rtest.Equals(t, uint64(0), sn.Summary.DataAdded)
			t.Logf("tree=%s data_blobs=%d tree_blobs=%d added_bytes=%d", sn.Tree.String(), sn.Summary.DataBlobs, sn.Summary.TreeBlobs, sn.Summary.DataAdded)
		})
	}
	t.Run("without-option", func(t *testing.T) {
		sn := backup(t, restore(t), BackupOptions{Parent: seedID})
		rtest.Assert(t, !head.Tree.Equal(*sn.Tree), "default backup unexpectedly suppressed native metadata")
	})
	cases := []struct {
		name string
		edit func(string)
	}{
		{"chmod", func(dir string) { rtest.OK(t, os.Chmod(filepath.Join(dir, "single"), 0600)) }},
		{"utime-nanosecond", func(dir string) {
			rtest.OK(t, os.Chtimes(filepath.Join(dir, "single"), mtime, mtime.Add(time.Nanosecond)))
		}},
		{"directory-utime", func(dir string) {
			rtest.OK(t, os.Chtimes(filepath.Join(dir, "sub"), mtime, mtime.Add(time.Nanosecond)))
		}},
		{"xattr", func(dir string) {
			rtest.OK(t, xattr.Set(filepath.Join(dir, "single"), "user.stabletree", []byte("changed")))
		}},
		{"symlink-retarget", func(dir string) {
			rtest.OK(t, os.Remove(filepath.Join(dir, "symlink")))
			rtest.OK(t, os.Symlink("c", filepath.Join(dir, "symlink")))
		}},
		{"hardlink-split", func(dir string) {
			rtest.OK(t, os.Remove(filepath.Join(dir, "sub", "b")))
			rtest.OK(t, os.WriteFile(filepath.Join(dir, "sub", "b"), []byte("data"), 0640))
			rtest.OK(t, os.Chmod(filepath.Join(dir, "sub", "b"), 0640))
			rtest.OK(t, os.Chtimes(filepath.Join(dir, "sub", "b"), mtime, mtime))
			rtest.OK(t, os.Chtimes(filepath.Join(dir, "sub"), mtime, mtime))
		}},
		{"hardlink-join", func(dir string) {
			for _, p := range []string{"c", "sub/d"} {
				rtest.OK(t, os.Remove(filepath.Join(dir, p)))
				rtest.OK(t, os.Link(filepath.Join(dir, "a"), filepath.Join(dir, p)))
			}
			rtest.OK(t, os.Chtimes(filepath.Join(dir, "sub"), mtime, mtime))
		}},
		{"same-size-same-mtime-rewrite", func(dir string) {
			rtest.OK(t, os.WriteFile(filepath.Join(dir, "single"), []byte("edit"), 0640))
			rtest.OK(t, os.Chtimes(filepath.Join(dir, "single"), mtime, mtime))
		}},
		{"hardlink-in-place-write", func(dir string) {
			rtest.OK(t, os.WriteFile(filepath.Join(dir, "a"), []byte("edit"), 0640))
			rtest.OK(t, os.Chtimes(filepath.Join(dir, "a"), mtime, mtime))
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dir := restore(t)
			// A raw image snapshot is the independent live-stat content parent.
			image := backup(t, dir, BackupOptions{Parent: seedID})
			test.edit(dir)
			currentOpts := opts
			currentOpts.Parent = image.ID().String()
			sn := backup(t, dir, currentOpts)
			rtest.Assert(t, !head.Tree.Equal(*sn.Tree), "%s falsely unchanged", test.name)
			out := t.TempDir()
			testRunRestore(t, env.gopts, out, sn.ID().String())
			assertGroups(t, out, test.name == "hardlink-join", test.name == "hardlink-split")
			if strings.Contains(test.name, "write") {
				p := "single"
				if test.name == "hardlink-in-place-write" {
					p = "a"
				}
				buf, err := os.ReadFile(filepath.Join(out, p))
				rtest.OK(t, err)
				rtest.Equals(t, "edit", string(buf))
			}
			if test.name == "chmod" {
				fi, err := os.Stat(filepath.Join(out, "single"))
				rtest.OK(t, err)
				rtest.Equals(t, os.FileMode(0600), fi.Mode().Perm())
			}
			if test.name == "xattr" {
				value, err := xattr.Get(filepath.Join(out, "single"), "user.stabletree")
				rtest.OK(t, err)
				rtest.Equals(t, "changed", string(value))
			}
			if test.name == "symlink-retarget" {
				target, err := os.Readlink(filepath.Join(out, "symlink"))
				rtest.OK(t, err)
				rtest.Equals(t, "c", target)
			}
			if strings.Contains(test.name, "utime") {
				p := "single"
				if test.name == "directory-utime" {
					p = "sub"
				}
				fi, err := os.Stat(filepath.Join(out, p))
				rtest.OK(t, err)
				rtest.Assert(t, fi.ModTime().Equal(mtime.Add(time.Nanosecond)), "mtime edit lost")
			}
			// A changed normalized snapshot is also a stable future baseline.
			rebased := backup(t, out, BackupOptions{Parent: sn.ID().String(), TreeMetadataParent: sn.ID().String()})
			rtest.Equals(t, sn.Tree, rebased.Tree)
		})
	}
	t.Run("skip-against-metadata-parent", func(t *testing.T) {
		dir := restore(t)
		image := backup(t, dir, BackupOptions{Parent: seedID})
		before := loadSnapshotMap(t, env.gopts)
		buf, err := withCaptureStdout(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
			cleanup := rtest.Chdir(t, dir)
			defer cleanup()
			gopts.JSON = true
			currentOpts := BackupOptions{Parent: image.ID().String(), TreeMetadataParent: seedID, SkipIfUnchanged: true}
			return runBackup(ctx, currentOpts, gopts, gopts.Term, []string{"."})
		})
		rtest.OK(t, err)
		rtest.Equals(t, before, loadSnapshotMap(t, env.gopts))
		var summary map[string]json.RawMessage
		for _, line := range strings.Split(buf.String(), "\n") {
			if strings.Contains(line, `"message_type":"summary"`) {
				rtest.OK(t, json.Unmarshal([]byte(line), &summary))
			}
		}
		rtest.Assert(t, summary != nil, "missing JSON summary: %s", buf.String())
		rtest.Assert(t, summary["snapshot_id"] == nil, "skip emitted snapshot ID")
		rtest.Equals(t, "0", string(summary["data_added"]))
		t.Log(strings.TrimSpace(buf.String()))
	})
	t.Run("revert-to-content-parent-is-changed", func(t *testing.T) {
		dir := restore(t)
		rtest.OK(t, os.WriteFile(filepath.Join(dir, "single"), []byte("edit"), 0640))
		rtest.OK(t, os.Chtimes(filepath.Join(dir, "single"), mtime, mtime))
		advanced := backup(t, dir, opts)
		reverted := restore(t)
		sn := backup(t, reverted, BackupOptions{Parent: seedID, TreeMetadataParent: advanced.ID().String(), SkipIfUnchanged: true})
		rtest.Assert(t, !advanced.Tree.Equal(*sn.Tree), "revert incorrectly skipped against the content parent")
	})
	t.Run("exact-parent-required", func(t *testing.T) {
		for _, invalid := range []string{"latest", seedID[:8], strings.Repeat("f", 64)} {
			err := testRunBackupAssumeFailure(t, src, []string{"."}, BackupOptions{TreeMetadataParent: invalid}, env.gopts)
			rtest.Assert(t, err != nil, "invalid/unavailable metadata parent %s accepted", invalid)
		}
	})
	testRunCheck(t, env.gopts)
}
