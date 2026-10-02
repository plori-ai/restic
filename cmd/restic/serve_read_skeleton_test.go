package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/restic/restic/internal/backend/mem"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
	"github.com/restic/restic/internal/walker"
)

// skeletonEnv is a local repository with a generated snapshot, opened a
// second time by a serve-read handler on a Unix socket.
type skeletonEnv struct {
	env    *testEnvironment
	repo   *repository.Repository
	id     restic.ID
	blobs  map[restic.ID][]byte
	h      *serveReadHandler
	socket string
}

func newSkeletonEnv(t *testing.T, spec genTreeSpec) *skeletonEnv {
	env, cleanup := withTestEnvironment(t)
	t.Cleanup(cleanup)
	// The reader lists index files on every reload.
	env.gopts.BackendTestHook = nil
	testRunInit(t, env.gopts)
	open := func() *repository.Repository {
		var repo *repository.Repository
		rtest.OK(t, withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) (err error) {
			repo, err = global.OpenRepository(ctx, gopts, &progress.NoopPrinter{})
			return err
		}))
		rtest.OK(t, repo.LoadIndex(context.Background(), nil))
		return repo
	}
	e := &skeletonEnv{env: env, repo: open()}
	start := time.Now()
	e.id, e.blobs = genTree(t, e.repo, spec)
	t.Logf("generated snapshot %s in %v", e.id.Str(), time.Since(start).Round(time.Millisecond))
	e.h = newServeReadHandler(open())
	e.socket = serveOnSocket(t, e.h)
	return e
}

// skeletonLsNode is one node line of `restic ls --json`.
type skeletonLsNode struct {
	MessageType string      `json:"message_type"`
	Path        string      `json:"path"`
	Type        string      `json:"type"`
	UID         uint32      `json:"uid"`
	GID         uint32      `json:"gid"`
	Size        *uint64     `json:"size"`
	Mode        os.FileMode `json:"mode"`
	ModTime     time.Time   `json:"mtime"`
	AccessTime  time.Time   `json:"atime"`
}

var lzfmTypeNames = map[uint64]string{1: "dir", 2: "file", 3: "symlink", 4: "fifo", 5: "chardev", 6: "dev", 7: "socket"}

// testMode is the protocol mode of an os.FileMode, written independently
// of lzfmMode.
func testMode(m os.FileMode) uint64 {
	v := uint64(m & 0o777)
	for bit, flag := range map[uint64]os.FileMode{0o4000: os.ModeSetuid, 0o2000: os.ModeSetgid, 0o1000: os.ModeSticky} {
		if m&flag != 0 {
			v |= bit
		}
	}
	return v
}

// checkSkeleton compares the decoded stream with `restic ls --json` and with
// an independent walk of the snapshot, field by field.
func checkSkeleton(t *testing.T, e *skeletonEnv, st *lzfmTestStream) {
	ctx := context.Background()
	sn, err := data.LoadSnapshot(ctx, e.repo, e.id)
	rtest.OK(t, err)
	rtest.Assert(t, bytes.Equal(st.Source, e.id[:]), "header source %x", st.Source)
	rtest.Assert(t, bytes.Equal(st.Root, sn.Tree[:]), "header root %x", st.Root)
	rtest.Assert(t, strings.Contains(st.Producer, "serve-read"), "producer %q", st.Producer)

	// Independent walk: same order, same fields.
	groups := map[uint64][2]uint64{} // stream group -> (device, inode)
	seen := map[[2]uint64]uint64{}
	i := 0
	devices := 0
	err = walker.Walk(ctx, e.repo, *sn.Tree, walker.WalkVisitor{ProcessNode: func(_ restic.ID, p string, n *data.Node, err error) error {
		if err != nil || n == nil {
			return err
		}
		if i >= len(st.Records) {
			return fmt.Errorf("stream has %d records, walk has more", len(st.Records))
		}
		r := st.Records[i]
		i++
		where := fmt.Sprintf("record %d %q", i-1, p)
		rtest.Equals(t, strings.TrimPrefix(p, "/"), string(r.Path), where)
		rtest.Equals(t, string(n.Type), lzfmTypeNames[r.Type], where)
		rtest.Equals(t, testMode(n.Mode), r.Mode, where)
		rtest.Equals(t, uint64(n.UID), r.UID, where)
		rtest.Equals(t, uint64(n.GID), r.GID, where)
		rtest.Equals(t, n.ModTime.UnixNano(), r.Mtime, where)
		rtest.Equals(t, n.AccessTime.UnixNano(), r.Atime, where)
		rtest.Equals(t, n.LinkTarget, string(r.LinkTarget), where)
		rtest.Equals(t, len(n.ExtendedAttributes), len(r.Xattrs), where)
		for k, x := range n.ExtendedAttributes {
			rtest.Equals(t, x.Name, string(r.Xattrs[k][0]), where)
			rtest.Assert(t, bytes.Equal(x.Value, r.Xattrs[k][1]), "%s xattr %s", where, x.Name)
		}
		if n.Type == data.NodeTypeFile {
			rtest.Equals(t, n.Size, r.Size, where)
			rtest.Equals(t, len(n.Content), len(r.Blobs), where)
			for k, id := range n.Content {
				rtest.Assert(t, bytes.Equal(id[:], r.Blobs[k].ID), "%s blob %d", where, k)
				rtest.Equals(t, uint64(len(e.blobs[id])), r.Blobs[k].Length, where)
			}
		} else {
			rtest.Equals(t, uint64(0), r.Size, where)
			rtest.Equals(t, 0, len(r.Blobs), where)
		}
		switch n.Type {
		case data.NodeTypeCharDev:
			rtest.Equals(t, [2]uint64{0x1234, 0x56789}, [2]uint64{r.DevMajor, r.DevMinor}, where)
			devices++
		case data.NodeTypeDev:
			rtest.Equals(t, [2]uint64{8, 1}, [2]uint64{r.DevMajor, r.DevMinor}, where)
			devices++
		default:
			rtest.Equals(t, [2]uint64{0, 0}, [2]uint64{r.DevMajor, r.DevMinor}, where)
		}
		if n.Type != data.NodeTypeDir && n.Links > 1 {
			key := [2]uint64{n.DeviceID, n.Inode}
			rtest.Assert(t, r.LinkGroup != 0, "%s: no link group", where)
			rtest.Equals(t, n.Links, r.LinkCount, where)
			if g, ok := seen[key]; ok {
				rtest.Equals(t, g, r.LinkGroup, where)
			} else {
				_, taken := groups[r.LinkGroup]
				rtest.Assert(t, !taken, "%s: group %d reused for another inode", where, r.LinkGroup)
				seen[key], groups[r.LinkGroup] = r.LinkGroup, key
			}
		} else {
			rtest.Equals(t, [2]uint64{0, 0}, [2]uint64{r.LinkGroup, r.LinkCount}, where)
		}
		return nil
	}})
	rtest.OK(t, err)
	rtest.Equals(t, len(st.Records), i)
	rtest.Assert(t, len(seen) > 0 && devices > 0, "fixture lacks hard links (%d) or devices (%d)", len(seen), devices)

	// restic ls --json: every node, with the fields ls prints.
	gopts := e.env.gopts
	gopts.JSON = true
	out := testRunLsWithOpts(t, gopts, LsOptions{}, []string{e.id.String()})
	byPath := make(map[string]*lzfmTestRecord, len(st.Records))
	raw := 0
	for k := range st.Records {
		if !utf8.Valid(st.Records[k].Path) {
			raw++
		}
		byPath["/"+string(st.Records[k].Path)] = &st.Records[k]
	}
	nodes, matched := 0, 0
	for _, line := range bytes.Split(out, []byte("\n")) {
		var n skeletonLsNode
		if len(line) == 0 {
			continue
		}
		rtest.OK(t, json.Unmarshal(line, &n))
		if n.MessageType != "node" {
			continue
		}
		nodes++
		r, ok := byPath[n.Path]
		if !ok {
			// ls replaces bytes that are not UTF-8; the walk above
			// compared those records byte for byte.
			rtest.Assert(t, strings.ContainsRune(n.Path, utf8.RuneError), "ls path %q not in the stream", n.Path)
			continue
		}
		matched++
		rtest.Equals(t, n.Type, lzfmTypeNames[r.Type], n.Path)
		rtest.Equals(t, testMode(n.Mode), r.Mode, n.Path)
		rtest.Equals(t, uint64(n.UID), r.UID, n.Path)
		rtest.Equals(t, uint64(n.GID), r.GID, n.Path)
		rtest.Equals(t, n.ModTime.UnixNano(), r.Mtime, n.Path)
		rtest.Equals(t, n.AccessTime.UnixNano(), r.Atime, n.Path)
		if n.Size != nil {
			rtest.Equals(t, *n.Size, r.Size, n.Path)
		}
	}
	rtest.Equals(t, len(st.Records), nodes)
	rtest.Assert(t, raw > 0 && matched+raw >= nodes, "ls matched %d of %d nodes, %d non-UTF-8 paths", matched, nodes, raw)
	t.Logf("compared %d records with the walk and %d with restic ls (%d paths are not UTF-8)", len(st.Records), matched, raw)
}

func testSkeletonShape(t *testing.T, spec genTreeSpec) {
	e := newSkeletonEnv(t, spec)
	var b []byte
	var times []time.Duration
	for k := 0; k < 3; k++ {
		var d time.Duration
		b, d = fetchSkeleton(t, e.socket, e.id)
		times = append(times, d)
	}
	start := time.Now()
	st, err := lzfmDecode(bytes.NewReader(b))
	rtest.OK(t, err)
	t.Logf("skeleton: %d records, %d bytes, request times %v, decode %v", len(st.Records), len(b), times, time.Since(start).Round(time.Millisecond))
	checkSkeleton(t, e, st)
}

func TestServeReadSkeleton50k(t *testing.T) {
	testSkeletonShape(t, genTreeSpec{top: 20, mid: 25, leaf: 100, seed: 50, blobs: 512, maxBlob: 16 << 10})
}

func TestServeReadSkeleton295k(t *testing.T) {
	if testing.Short() {
		t.Skip("295k-entry tree in -short mode")
	}
	testSkeletonShape(t, genTreeSpec{top: 40, mid: 50, leaf: 147, seed: 295, blobs: 512, maxBlob: 16 << 10})
}

// TestServeReadSkeletonFailsClosed checks that a snapshot the stream cannot
// describe losslessly ends with an error trailer, and the refusals before
// the stream starts.
func TestServeReadSkeletonFailsClosed(t *testing.T) {
	ctx := context.Background()
	repo, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{})
	var content restic.ID
	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) (err error) {
		content, _, _, err = up.SaveBlob(ctx, restic.DataBlob, []byte("12345"), restic.ID{}, false)
		return err
	}))
	snapshot := func(nodes ...*data.Node) restic.ID {
		var root restic.ID
		rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) (err error) {
			root, err = data.SaveTree(ctx, up, func(yield func(data.NodeOrError) bool) {
				for _, n := range nodes {
					if !yield(data.NodeOrError{Node: n}) {
						return
					}
				}
			})
			return err
		}))
		id, err := data.SaveSnapshot(ctx, repo, &data.Snapshot{Tree: &root, Time: time.Unix(1, 0)})
		rtest.OK(t, err)
		return id
	}
	good := &data.Node{Name: "a", Type: data.NodeTypeFile, Mode: 0o644, Size: 5, Content: restic.IDs{content}}
	cases := map[string]restic.ID{
		"irregular":  snapshot(good, &data.Node{Name: "b", Type: data.NodeTypeIrregular, Mode: 0o644}),
		"empty link": snapshot(good, &data.Node{Name: "b", Type: data.NodeTypeSymlink, Mode: os.ModeSymlink | 0o777}),
		"size":       snapshot(good, &data.Node{Name: "b", Type: data.NodeTypeFile, Mode: 0o644, Size: 6, Content: restic.IDs{content}}),
		"slash":      snapshot(good, &data.Node{Name: "b/c", Type: data.NodeTypeFile, Mode: 0o644}),
		"dot":        snapshot(&data.Node{Name: "..", Type: data.NodeTypeFile, Mode: 0o644}),
		"no subtree": snapshot(good, &data.Node{Name: "d", Type: data.NodeTypeDir, Mode: os.ModeDir | 0o755}),
		"no blob":    snapshot(good, &data.Node{Name: "b", Type: data.NodeTypeFile, Mode: 0o644, Size: 5, Content: restic.IDs{restic.NewRandomID()}}),
	}
	reader := repository.TestOpenBackend(t, be)
	rtest.OK(t, reader.LoadIndex(ctx, nil))
	h := newServeReadHandler(reader)
	for name, id := range cases {
		w := serveReadRequest(h, "GET", "/skeleton?snapshot="+id.String(), "")
		rtest.Equals(t, http.StatusOK, w.Code)
		_, err := lzfmDecode(bytes.NewReader(w.Body.Bytes()))
		rtest.Assert(t, err != nil && strings.Contains(err.Error(), "failure"), "%s: %v", name, err)
	}
	ok := serveReadRequest(h, "GET", "/skeleton?snapshot="+snapshot(good).String(), "")
	st, err := lzfmDecode(bytes.NewReader(ok.Body.Bytes()))
	rtest.OK(t, err)
	rtest.Equals(t, 1, len(st.Records))
	rtest.Equals(t, http.StatusNotFound, serveReadRequest(h, "GET", "/skeleton?snapshot="+restic.NewRandomID().String(), "").Code)
	rtest.Equals(t, http.StatusBadRequest, serveReadRequest(h, "GET", "/skeleton?snapshot="+content.Str(), "").Code)
	rtest.Equals(t, http.StatusMethodNotAllowed, serveReadRequest(h, "POST", "/skeleton?snapshot="+content.String(), "").Code)
}

// TestServeReadSkeletonLoadsTreesConcurrently streams a snapshot of 106
// trees from a backend without a repository cache that answers each read
// after 20 ms. One tree at a time takes at least 2.1 s.
func TestServeReadSkeletonLoadsTreesConcurrently(t *testing.T) {
	writer, raw := repository.TestRepositoryWithBackend(t, mem.New(), 0, repository.Options{})
	id, _ := genTree(t, writer, genTreeSpec{top: 4, mid: 25, leaf: 10, seed: 7, blobs: 16, maxBlob: 1 << 10})
	var loads int64
	reader := repository.TestOpenBackend(t, &latencyBackend{Backend: raw, count: &loads, delay: 20 * time.Millisecond})
	rtest.OK(t, reader.LoadIndex(context.Background(), nil))
	socket := serveOnSocket(t, newServeReadHandler(reader))
	b, took := fetchSkeleton(t, socket, id)
	st, err := lzfmDecode(bytes.NewReader(b))
	rtest.OK(t, err)
	t.Logf("%d records, %d tree reads in %v", len(st.Records), atomic.LoadInt64(&loads), took.Round(time.Millisecond))
	rtest.Equals(t, int64(106), atomic.LoadInt64(&loads))
	rtest.Assert(t, took < time.Second, "skeleton took %v", took)
}
