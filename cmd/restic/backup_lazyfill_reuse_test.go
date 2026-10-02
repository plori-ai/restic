//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
	"github.com/restic/restic/internal/walker"
	"github.com/spf13/pflag"
)

type reuseTestRecord struct {
	path  string
	ino   uint64
	size  uint64
	blobs restic.IDs
	lens  []uint64
}

// encodeReuseMap writes a reuse map (LZFR version 1).
func encodeReuseMap(binding []byte, recs []reuseTestRecord) []byte {
	return encodeReuseMapCount(binding, recs, uint64(len(recs)))
}

// encodeReuseMapCount writes a reuse map whose trailer says count records.
func encodeReuseMapCount(binding []byte, recs []reuseTestRecord, count uint64) []byte {
	var buf bytes.Buffer
	e := newLZFEncoder(&buf, lzfrMagic)
	e.reset()
	if len(binding) > 0 {
		e.bytes(1, binding)
	}
	_ = e.frame(lzfmFrameHeader)
	for _, r := range recs {
		e.reset()
		e.bytes(1, []byte(r.path))
		e.uint(2, r.ino)
		e.uint(3, r.size)
		for i, id := range r.blobs {
			e.blob(4, id, r.lens[i])
		}
		_ = e.frame(lzfmFrameRecord)
	}
	e.reset()
	e.uint(1, count)
	e.bytes(2, e.h.Sum(nil))
	_ = e.frame(lzfmFrameTrailer)
	_ = e.w.Flush()
	return buf.Bytes()
}

// reuseFD returns a file descriptor (3 or larger) that reads b from the
// start; runBackup closes it.
func reuseFD(t *testing.T, b []byte) int {
	f, err := os.CreateTemp(t.TempDir(), "reuse")
	rtest.OK(t, err)
	_, err = f.Write(b)
	rtest.OK(t, err)
	_, err = f.Seek(0, 0)
	rtest.OK(t, err)
	fd, err := syscall.Dup(int(f.Fd()))
	rtest.OK(t, err)
	rtest.OK(t, f.Close())
	return fd
}

func reuseOptions(t *testing.T, args ...string) BackupOptions {
	var opts BackupOptions
	f := pflag.NewFlagSet("backup", pflag.ContinueOnError)
	opts.AddFlags(f)
	rtest.OK(t, f.Parse(args))
	return opts
}

func inodeOf(t *testing.T, p string) uint64 {
	fi, err := os.Lstat(p)
	rtest.OK(t, err)
	return fi.Sys().(*syscall.Stat_t).Ino
}

// snapshotFiles maps the regular files of a snapshot (relative paths) to
// their nodes.
func snapshotFiles(t *testing.T, gopts global.Options, id string) map[string]*data.Node {
	var files map[string]*data.Node
	rtest.OK(t, withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		repo, err := global.OpenRepository(ctx, gopts, &progress.NoopPrinter{})
		if err != nil {
			return err
		}
		if err = repo.LoadIndex(ctx, nil); err != nil {
			return err
		}
		sn, err := data.LoadSnapshot(ctx, repo, restic.TestParseID(id))
		if err != nil {
			return err
		}
		files = map[string]*data.Node{}
		return walker.Walk(ctx, repo, *sn.Tree, walker.WalkVisitor{ProcessNode: func(_ restic.ID, p string, n *data.Node, err error) error {
			if err == nil && n != nil && n.Type == data.NodeTypeFile {
				files[strings.TrimPrefix(p, "/")] = n
			}
			return err
		}})
	}))
	return files
}

func newestSnapshot(t *testing.T, gopts global.Options, before map[string]struct{}) string {
	_, id := lastSnapshot(before, loadSnapshotMap(t, gopts))
	return id
}

// TestBackupLazyfillReuseMap checks the reuse map of --lazyfill-reuse-map-fd:
// a matching record's blobs are stored without reading the file (a lying map
// is visible, and a file without read permission is stored); a record whose
// inode, size or blobs do not match, an unlisted file, and every file of a
// map with another binding or a malformed map are read; parent metadata never
// substitutes for a read; without the flags the stock comparison applies.
func TestBackupLazyfillReuseMap(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads files without read permission")
	}
	env, cleanup := withTestEnvironment(t)
	defer cleanup()
	env.gopts.BackendTestHook = nil
	testRunInit(t, env.gopts)
	tree := filepath.Join(env.testdata, "tree")
	content := map[string][]byte{}
	for i, name := range []string{"donor", "listed", "locked", "wrong-inode", "wrong-size", "missing-blob", "unlisted", "rewrite", "raw-\xff\xfe", "sub/dir/nested"} {
		content[name] = bytes.Repeat([]byte{byte('a' + i)}, 4096)
		rtest.OK(t, os.MkdirAll(filepath.Join(tree, filepath.Dir(name)), 0o755))
		rtest.OK(t, os.WriteFile(filepath.Join(tree, name), content[name], 0o644))
	}
	stamp := time.Unix(1_700_000_000, 123456789)
	for name := range content {
		rtest.OK(t, os.Chtimes(filepath.Join(tree, name), stamp, stamp))
	}
	before := loadSnapshotMap(t, env.gopts)
	testRunBackup(t, tree, []string{"."}, BackupOptions{}, env.gopts)
	base := newestSnapshot(t, env.gopts, before)
	baseFiles := snapshotFiles(t, env.gopts, base)
	donor := baseFiles["donor"].Content

	// A same-size rewrite that restores the mtime, and a larger file.
	content["rewrite"] = bytes.Repeat([]byte{'R'}, 4096)
	rtest.OK(t, os.WriteFile(filepath.Join(tree, "rewrite"), content["rewrite"], 0o644))
	rtest.OK(t, os.Chtimes(filepath.Join(tree, "rewrite"), stamp, stamp))
	content["wrong-size"] = bytes.Repeat([]byte{'S'}, 5000)
	rtest.OK(t, os.WriteFile(filepath.Join(tree, "wrong-size"), content["wrong-size"], 0o644))

	binding := []byte("job-1/attempt-2")
	record := func(name string) reuseTestRecord {
		return reuseTestRecord{path: name, ino: inodeOf(t, filepath.Join(tree, name)), size: 4096, blobs: donor, lens: []uint64{4096}}
	}
	recs := []reuseTestRecord{record("listed"), record("locked"), record("wrong-inode"), record("wrong-size"),
		record("missing-blob"), record("raw-\xff\xfe"), record("sub/dir/nested")}
	recs[2].ino++
	recs[4].blobs = restic.IDs{restic.NewRandomID()}
	mapBytes := encodeReuseMap(binding, recs)
	bindingArg := base64.StdEncoding.EncodeToString(binding)

	backup := func(extra ...string) map[string]*data.Node {
		t.Helper()
		before := loadSnapshotMap(t, env.gopts)
		opts := reuseOptions(t, append([]string{"--parent", base, "--ignore-inode", "--ignore-ctime"}, extra...)...)
		testRunBackup(t, tree, []string{"."}, opts, env.gopts)
		return snapshotFiles(t, env.gopts, newestSnapshot(t, env.gopts, before))
	}
	rtest.OK(t, os.Chmod(filepath.Join(tree, "locked"), 0))
	files := backup("--lazyfill-reuse-map-fd", strconv.Itoa(reuseFD(t, mapBytes)), "--lazyfill-reuse-binding", bindingArg)
	rtest.OK(t, os.Chmod(filepath.Join(tree, "locked"), 0o644))
	for _, name := range []string{"listed", "locked", "raw-\xff\xfe", "sub/dir/nested"} {
		rtest.Equals(t, donor, files[name].Content, "reused "+name)
		rtest.Equals(t, uint64(4096), files[name].Size, "reused "+name)
	}
	// Metadata is the file's own: the unreadable file keeps mode 0.
	rtest.Equals(t, os.FileMode(0), files["locked"].Mode.Perm())
	fi, err := os.Lstat(filepath.Join(tree, "listed"))
	rtest.OK(t, err)
	rtest.Equals(t, fi.Mode().Perm(), files["listed"].Mode.Perm())
	rtest.Equals(t, inodeOf(t, filepath.Join(tree, "listed")), files["listed"].Inode)
	for _, name := range []string{"wrong-inode", "missing-blob", "unlisted", "donor"} {
		rtest.Equals(t, baseFiles[name].Content, files[name].Content, "read "+name)
	}
	rtest.Assert(t, !slices.Equal(files["wrong-size"].Content, donor) && files["wrong-size"].Size == 5000, "wrong-size was reused")
	rtest.Assert(t, !slices.Equal(files["rewrite"].Content, baseFiles["rewrite"].Content), "parent metadata substituted for reading the rewrite")

	// Another binding, a malformed map, an empty map: no file is reused, and
	// parent metadata still does not substitute for a read.
	for name, args := range map[string][]string{
		"binding":   {"--lazyfill-reuse-map-fd", strconv.Itoa(reuseFD(t, mapBytes)), "--lazyfill-reuse-binding", base64.StdEncoding.EncodeToString([]byte("job-1/attempt-3"))},
		"malformed": {"--lazyfill-reuse-map-fd", strconv.Itoa(reuseFD(t, mapBytes[:len(mapBytes)-1])), "--lazyfill-reuse-binding", bindingArg},
		"empty":     {"--lazyfill-reuse-map-fd", strconv.Itoa(reuseFD(t, encodeReuseMap(binding, nil))), "--lazyfill-reuse-binding", bindingArg},
	} {
		files := backup(args...)
		for _, n := range []string{"listed", "locked", "raw-\xff\xfe", "sub/dir/nested"} {
			rtest.Equals(t, baseFiles[n].Content, files[n].Content, name+": "+n)
		}
		rtest.Assert(t, !slices.Equal(files["rewrite"].Content, baseFiles["rewrite"].Content), "%s: rewrite not read", name)
	}

	// Stock behaviour without the flags: the parent's metadata matches, so
	// the rewrite keeps the parent's blobs.
	files = backup()
	rtest.Equals(t, baseFiles["rewrite"].Content, files["rewrite"].Content)

	// Flag validation.
	for _, args := range [][]string{
		{"--lazyfill-reuse-map-fd", "5"},
		{"--lazyfill-reuse-binding", bindingArg},
		{"--lazyfill-reuse-map-fd", "2", "--lazyfill-reuse-binding", bindingArg},
		{"--lazyfill-reuse-map-fd", "5", "--lazyfill-reuse-binding", "not base64!"},
	} {
		err := testRunBackupAssumeFailure(t, tree, []string{"."}, reuseOptions(t, args...), env.gopts)
		rtest.Assert(t, err != nil && strings.Contains(err.Error(), "lazyfill-reuse"), "%v: %v", args, err)
	}
	err = testRunBackupAssumeFailure(t, tree, []string{"sub"}, reuseOptions(t, "--lazyfill-reuse-map-fd", "5", "--lazyfill-reuse-binding", bindingArg), env.gopts)
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "single target"), "target sub: %v", err)
}

// TestReadLazyfillReuseRejects checks that every malformed map is refused.
func TestReadLazyfillReuseRejects(t *testing.T) {
	id := restic.NewRandomID()
	good := reuseTestRecord{path: "a", ino: 1, size: 3, blobs: restic.IDs{id}, lens: []uint64{3}}
	m, err := readLazyfillReuse(bytes.NewReader(encodeReuseMap([]byte("b"), []reuseTestRecord{good})), []byte("b"))
	rtest.OK(t, err)
	rtest.Equals(t, 1, len(m))
	bad := map[string][]byte{
		"sum":       encodeReuseMap(nil, []reuseTestRecord{{path: "a", ino: 1, size: 4, blobs: restic.IDs{id}, lens: []uint64{3}}}),
		"no blobs":  encodeReuseMap(nil, []reuseTestRecord{{path: "a", ino: 1, size: 3}}),
		"dot path":  encodeReuseMap(nil, []reuseTestRecord{{path: "a/../b", ino: 1, size: 3, blobs: restic.IDs{id}, lens: []uint64{3}}}),
		"duplicate": encodeReuseMap(nil, []reuseTestRecord{good, good}),
		"magic":     append([]byte("LZFM"), encodeReuseMap(nil, nil)[4:]...),
		"version":   append([]byte("LZFR\x02"), encodeReuseMap(nil, nil)[5:]...),
		"trailing":  append(encodeReuseMap(nil, nil), 0),
	}
	for name, b := range bad {
		_, err := readLazyfillReuse(bytes.NewReader(b), nil)
		rtest.Assert(t, err != nil, "%s accepted", name)
	}
	_, err = readLazyfillReuse(bytes.NewReader(encodeReuseMapCount(nil, []reuseTestRecord{good}, 2)), nil)
	rtest.Assert(t, err != nil, "wrong count accepted")
	_, err = readLazyfillReuse(bytes.NewReader(encodeReuseMap([]byte("x"), nil)), []byte("y"))
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "binding"), "binding mismatch: %v", err)
}
