package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/checker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

// faultBackend fails or blocks pack uploads.
type faultBackend struct {
	backend.Backend
	mu        sync.Mutex
	failPacks int
	block     bool
	blocked   chan struct{}
	once      sync.Once
}

func (b *faultBackend) Save(ctx context.Context, h backend.Handle, rd backend.RewindReader) error {
	if h.Type == backend.PackFile {
		b.mu.Lock()
		fail, block := b.failPacks > 0, b.block
		if fail {
			b.failPacks--
		}
		b.mu.Unlock()
		if fail {
			return errors.New("injected pack upload failure")
		}
		if block {
			b.once.Do(func() { close(b.blocked) })
			<-ctx.Done()
			return ctx.Err()
		}
	}
	return b.Backend.Save(ctx, h, rd)
}

func newFaultFixture(t *testing.T) (*swFixture, *faultBackend) {
	fb := &faultBackend{Backend: repository.TestBackend(t)}
	f := newSWFixtureOn(t, fb)
	return f, fb
}

// A failed upload leaves upstream's uploader state behind; the next write
// opens the repository again in the same process instead of answering 503
// until a restart.
func TestServeWriteUploadFailureRecovery(t *testing.T) {
	f, fb := newFaultFixture(t)
	base := f.backup(nil, false)
	fb.failPacks = 1
	req := f.request(base, wr("a/new", "bytes after a failure"))
	r := f.post("/tree-write", req)
	rtest.Equals(t, http.StatusInternalServerError, r.code)
	rtest.Equals(t, "write_failed", r.failure.Code)
	rtest.Assert(t, f.srv.broken, "failure did not mark the uploader broken")
	rtest.Equals(t, 0, f.countFiles(restic.LockFile))
	resp := f.write(req)
	rtest.Assert(t, !f.srv.broken, "still broken after a successful write")
	rtest.Equals(t, "bytes after a failure", string(f.readFile(resp.Head.Snapshot, "a/new")))
	resp = f.edit(mustID(t, resp.Head.Snapshot), wr("a/new", "and again"))
	rtest.Equals(t, "and again", string(f.readFile(resp.Head.Snapshot, "a/new")))
	// The failed request's other pack can have been stored without an index
	// entry: prune removes such orphans. Nothing else is wrong.
	rtest.Assert(t, checkRepoOrphans(t, f.repo) <= 1, "more than one orphan pack")
}

// checkRepoOrphans is checker.TestCheckRepo, except that packs no index
// references are counted instead of failing the test.
func checkRepoOrphans(t *testing.T, repo *repository.Repository) int {
	t.Helper()
	chkr := checker.New(repo, true)
	hints, errs := chkr.LoadIndex(context.TODO(), nil)
	rtest.Assert(t, len(errs) == 0 && len(hints) == 0, "index: %v %v", errs, hints)
	rtest.OK(t, chkr.LoadSnapshots(context.TODO(), &data.SnapshotFilter{}, nil))
	orphans := 0
	collect := func(run func(chan<- error)) {
		errChan := make(chan error)
		go run(errChan)
		for err := range errChan {
			var pe *repository.PackError
			if errors.As(err, &pe) && pe.Orphaned {
				orphans++
				continue
			}
			t.Error(err)
		}
	}
	collect(func(c chan<- error) { chkr.Packs(context.TODO(), c) })
	collect(func(c chan<- error) { chkr.Structure(context.TODO(), nil, c) })
	blobs, err := chkr.UnusedBlobs(context.TODO())
	rtest.OK(t, err)
	rtest.Assert(t, len(blobs) == 0, "unused blobs: %v", blobs)
	collect(func(c chan<- error) {
		chkr.ReadPacks(context.TODO(), func(packs map[restic.ID]int64) map[restic.ID]int64 { return packs }, nil, c)
	})
	return orphans
}

func (f *swFixture) readFile(snapshot, p string) []byte {
	f.t.Helper()
	n := f.flatten(snapshot)[p]
	rtest.Assert(f.t, n != nil, "%s missing", p)
	var out []byte
	for _, id := range n.Content {
		b, err := f.repo.LoadBlob(context.TODO(), restic.DataBlob, id, nil)
		rtest.OK(f.t, err)
		out = append(out, b...)
	}
	return out
}

// An exclusive (maintenance) lock refuses a write; the process holds no lock
// while idle or after a request.
func TestServeWriteLockPerRequest(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	exclusive, _, err := repository.Lock(context.TODO(), f.repo, true, 0, func(string) {}, func(string, ...interface{}) {})
	rtest.OK(t, err)
	r := f.post("/tree-write", f.request(base, wr("a/b/file2", "x")))
	rtest.Equals(t, http.StatusServiceUnavailable, r.code)
	rtest.Equals(t, "repository_locked", r.failure.Code)
	exclusive.Unlock()
	rtest.Equals(t, 0, f.countFiles(restic.LockFile))
	resp := f.edit(base, wr("a/b/file2", "locked"))
	_, ok := resp.TimingsMS["index"]
	rtest.Assert(t, ok, "no index timing")
	rtest.Equals(t, 0, f.countFiles(restic.LockFile))
}

// SIGTERM cancels the server context: a write blocked in its upload is
// canceled and joined, and its lock is removed before drain returns. The next
// write in a new server succeeds.
func TestServeWriteDrainReleasesLock(t *testing.T) {
	f, fb := newFaultFixture(t)
	base := f.backup(nil, false)
	fb.block, fb.blocked = true, make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	socket := filepath.Join(t.TempDir(), "w.sock")
	served := make(chan error, 1)
	go func() { served <- serveReadListen(ctx, socket, f.srv) }()
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
	body, err := json.Marshal(f.request(base, wr("a/blocked", "never stored")))
	rtest.OK(t, err)
	answered := make(chan error, 1)
	go func() {
		for {
			if _, err := os.Lstat(socket); err == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		resp, err := client.Post("http://serve-write/tree-write", "application/json", bytes.NewReader(body))
		if err == nil {
			_ = resp.Body.Close()
		}
		answered <- err
	}()
	select {
	case <-fb.blocked:
	case <-time.After(30 * time.Second):
		t.Fatal("upload never started")
	}
	rtest.Equals(t, 1, f.countFiles(restic.LockFile))
	cancel()
	rtest.OK(t, <-served)
	f.srv.drain()
	<-answered
	rtest.Equals(t, 0, f.countFiles(restic.LockFile))
	rtest.Equals(t, 1, f.countFiles(restic.SnapshotFile))
	fb.mu.Lock()
	fb.block = false
	fb.mu.Unlock()
	f.srv = newTestServeWriteServer(t, f.be)
	f.edit(base, wr("a/after", "stored"))
	checker.TestCheckRepo(t, f.repo)
}

// The verifier rereads the candidate from the backend through its own index:
// a missing twin snapshot, a missing tree pack or a missing index file is
// found, and so is a result that does not match the request.
func TestServeWriteVerifierDetects(t *testing.T) {
	setup := func(t *testing.T) (*swFixture, treeWriteRequest, treeWriteResponse) {
		f := newSWFixture(t)
		rtest.OK(t, os.Remove(filepath.Join(f.dir, ".plori-trash/old")))
		base := f.backup(nil, false)
		before := restic.NewIDSet()
		rtest.OK(t, f.repo.List(context.TODO(), restic.IndexFile, func(id restic.ID, _ int64) error { before.Insert(id); return nil }))
		req := f.request(base, wr("a/new", "new content"), ed(writeEdit{Op: "rename", Path: "a/b/file2", To: "z/moved"}))
		resp := f.write(req)
		rtest.Assert(t, resp.Public != nil && !resp.Public.Empty, "twin missing")
		return f, req, resp
	}
	remove := func(t *testing.T, f *swFixture, tpe backend.FileType, name string) {
		rtest.OK(t, f.be.Remove(context.TODO(), backend.Handle{Type: tpe, Name: name}))
	}
	expect := func(t *testing.T, f *swFixture, req treeWriteRequest, resp treeWriteResponse, code string) {
		t.Helper()
		v := f.verify(req, resp)
		rtest.Assert(t, !v.OK && v.Code == code, "want %s, got ok=%v %s: %s", code, v.OK, v.Code, v.Detail)
	}
	t.Run("twin", func(t *testing.T) {
		f, req, resp := setup(t)
		remove(t, f, backend.SnapshotFile, resp.Public.Snapshot)
		expect(t, f, req, resp, "snapshot_missing")
	})
	t.Run("tree", func(t *testing.T) {
		f, req, resp := setup(t)
		rtest.OK(t, f.repo.LoadIndex(context.TODO(), nil))
		packs := f.repo.LookupBlob(restic.TreeBlob, mustID(t, resp.Head.Tree))
		rtest.Assert(t, len(packs) > 0, "head tree not indexed")
		remove(t, f, backend.PackFile, packs[0].PackID.String())
		expect(t, f, req, resp, "tree_unreadable")
	})
	t.Run("index", func(t *testing.T) {
		f, req, resp := setup(t)
		rtest.OK(t, f.repo.LoadIndex(context.TODO(), nil))
		packs := f.repo.LookupBlob(restic.TreeBlob, mustID(t, resp.Head.Tree))
		var names []string
		rtest.OK(t, f.repo.List(context.TODO(), restic.IndexFile, func(id restic.ID, _ int64) error { names = append(names, id.String()); return nil }))
		// Remove every index file that lists the new head tree's pack.
		for _, name := range names {
			buf, err := f.repo.LoadUnpacked(context.TODO(), restic.IndexFile, mustID(t, name))
			rtest.OK(t, err)
			if bytes.Contains(buf, []byte(packs[0].PackID.String())) {
				remove(t, f, backend.IndexFile, name)
			}
		}
		expect(t, f, req, resp, "blob_missing")
	})
	t.Run("result", func(t *testing.T) {
		f, req, resp := setup(t)
		bad := resp
		bad.Head.Entries++
		expect(t, f, req, bad, "count_mismatch")
		bad = resp
		bad.Head.Tree = resp.Public.Tree
		expect(t, f, req, bad, "plan_mismatch")
		bad = resp
		bad.DataAdded++
		expect(t, f, req, bad, "snapshot_mismatch")
		bad = resp
		bad.Contents = []contentReceipt{{IDs: []string{restic.Hash([]byte("other")).String()}}}
		expect(t, f, req, bad, "blob_missing")
		bad = resp
		pub := *resp.Public
		pub.Snapshot = resp.Head.Snapshot
		bad.Public = &pub
		expect(t, f, req, bad, "snapshot_mismatch")
		// The verifier still accepts the true result after the refusals.
		f.verifyOK(req, resp)
	})
}

// Index files replaced by maintenance (here `repair index`, as prune does)
// drop the caches that can name removed trees: the projection cache, the token
// index and the loaded snapshots.
func TestServeWriteIndexReplacementDropsCaches(t *testing.T) {
	f := newSWFixture(t)
	rtest.OK(t, os.Remove(filepath.Join(f.dir, ".plori-trash/old")))
	base := f.backup(nil, false)
	resp := f.edit(base, wr("a/one", "1"))
	projection := f.srv.w.public
	resp = f.edit(mustID(t, resp.Head.Snapshot), wr("a/two", "2"))
	rtest.Assert(t, f.srv.w.public == projection, "projection cache dropped without maintenance")
	rtest.OK(t, repository.RepairIndex(context.TODO(), f.repo, repository.RepairIndexOptions{}, &progress.NoopPrinter{}))
	resp = f.edit(mustID(t, resp.Head.Snapshot), wr("a/three", "3"))
	rtest.Assert(t, f.srv.w.public != projection, "projection cache kept after the index files were replaced")
	rtest.Assert(t, resp.Public != nil && !resp.Public.Empty, "twin missing")
	checker.TestCheckRepo(t, f.repo)
}
