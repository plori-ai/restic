package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/checker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// testExcludes is the example private-name list of doc/plori-tree-write.md.
var testExcludes = []string{"lost+found", ".nfs*", ".forge", ".trash", ".plori-trash", ".plori-workspace",
	".control", ".config", ".jfs", ".stats", ".accesslog"}

const testTrash = ".plori-trash"

func testServeWriteConfig() serveWriteConfig {
	return serveWriteConfig{excludes: testExcludes, trashDir: testTrash, maxRequest: 96 << 20}
}

// newTestServeWriteServer serves a backend through separate repository
// handles for the writer and the verifier, as the command opens them.
func newTestServeWriteServer(t *testing.T, be backend.Backend) *serveWriteServer {
	t.Helper()
	restic.TestSetLockTimeout(t, 0)
	open := func(ctx context.Context, _ bool) (*repository.Repository, error) {
		repo := repository.TestOpenBackend(t, be)
		return repo, repo.LoadIndex(ctx, nil)
	}
	srv, err := newServeWriteServer(context.TODO(), open, testServeWriteConfig())
	rtest.OK(t, err)
	return srv
}

// swFixture is a real directory backed up with the archiver (the helper's
// `restic backup .`), the repository and a serve-write server on it. The
// same edits are applied to the directory with the helper's POSIX operations
// (workspacehelper/mutation.go) and backed up again as the reference.
type swFixture struct {
	t     *testing.T
	dir   string
	repo  *repository.Repository
	be    backend.Backend
	srv   *serveWriteServer
	owner [2]uint32
	clock time.Time
}

func swWrite(t *testing.T, p, content string, mode os.FileMode) {
	t.Helper()
	rtest.OK(t, os.MkdirAll(filepath.Dir(p), 0755))
	rtest.OK(t, os.WriteFile(p, []byte(content), 0600))
	rtest.OK(t, os.Chmod(p, mode))
}

func newSWFixtureOn(t *testing.T, be backend.Backend) *swFixture {
	dir := t.TempDir()
	for p, c := range map[string]string{
		"a/b/file1": "one", "a/b/file2": "two two", "a/x": "linked to trash", "c/d/e/deep": "deep",
		".forge/state": "private", "empty": "", "a/mode": "mode",
	} {
		swWrite(t, filepath.Join(dir, p), c, 0644)
	}
	rtest.OK(t, os.Link(filepath.Join(dir, "a/b/file1"), filepath.Join(dir, "a/b/link1")))
	rtest.OK(t, os.MkdirAll(filepath.Join(dir, ".plori-trash"), 0700))
	rtest.OK(t, os.Link(filepath.Join(dir, "a/x"), filepath.Join(dir, ".plori-trash/old")))
	rtest.OK(t, os.Symlink("b/file1", filepath.Join(dir, "a/sym")))
	if err := syscall.Setxattr(filepath.Join(dir, "a/mode"), "user.plori", []byte("xattr"), 0); err != nil {
		t.Logf("no user xattrs: %v", err)
	}
	for _, d := range []string{"a", "a/b", "c", "c/d", "c/d/e", ".forge"} {
		rtest.OK(t, os.Chmod(filepath.Join(dir, d), 0755))
	}
	repo, be := repository.TestRepositoryWithBackend(t, be, 0, repository.Options{})
	return &swFixture{t: t, dir: dir, repo: repo, be: be, srv: newTestServeWriteServer(t, be),
		owner: [2]uint32{uint32(os.Getuid()), uint32(os.Getgid())}, clock: time.Now()}
}

func newSWFixture(t *testing.T) *swFixture { return newSWFixtureOn(t, nil) }

func (f *swFixture) backup(parent *restic.ID, public bool) restic.ID {
	f.t.Helper()
	back := rtest.Chdir(f.t, f.dir)
	defer back()
	arch := archiver.New(f.repo, fs.Local{}, archiver.Options{})
	if public {
		arch.SelectByName = func(item string) bool { return !f.srv.w.excluded(filepath.Base(item)) || item == f.dir }
	}
	opts := archiver.SnapshotOptions{Time: time.Now(), Hostname: "plori-workspace", Tags: data.TagList{"plori-op:ref"}}
	if parent != nil {
		sn, err := data.LoadSnapshot(context.TODO(), f.repo, *parent)
		rtest.OK(f.t, err)
		opts.ParentSnapshot = sn
	}
	_, id, _, err := arch.Snapshot(context.TODO(), []string{"."}, opts)
	rtest.OK(f.t, err)
	return id
}

type swResult struct {
	code    int
	resp    treeWriteResponse
	refusal writeRefusal
	failure failureBody
	body    string
}

func (f *swFixture) post(uri string, req any) swResult {
	f.t.Helper()
	body, err := json.Marshal(req)
	rtest.OK(f.t, err)
	w := serveReadRequest(f.srv, http.MethodPost, uri, string(body))
	out := swResult{code: w.Code, body: w.Body.String()}
	switch w.Code {
	case http.StatusOK:
		rtest.OK(f.t, json.Unmarshal(w.Body.Bytes(), &out.resp))
	case http.StatusConflict:
		rtest.OK(f.t, json.Unmarshal(w.Body.Bytes(), &out.refusal))
	default:
		rtest.OK(f.t, json.Unmarshal(w.Body.Bytes(), &out.failure))
	}
	return out
}

// testEdit is an edit with its content bytes, which request assigns to a
// content index.
type testEdit struct {
	writeEdit
	data *string
}

func wr(p, content string) testEdit {
	return testEdit{writeEdit: writeEdit{Op: "write", Path: p}, data: &content}
}

func ed(e writeEdit) testEdit { return testEdit{writeEdit: e} }

// request builds a tree-write request. Every request of a fixture gets a later
// time, as successive Files edits do.
func (f *swFixture) request(base restic.ID, edits ...testEdit) treeWriteRequest {
	f.clock = f.clock.Add(time.Second)
	req := treeWriteRequest{Version: treeWriteVersion, Time: f.clock.Format(time.RFC3339Nano), Owner: &f.owner, Hostname: "plori-workspace",
		Tags: []string{"plori-op:op-1", "plori-attempt:1"}, PublicTags: []string{"plori-public"}}
	if base.IsNull() {
		req.Base, req.Paths = treeSource{Empty: true}, []string{"/scan"}
	} else {
		req.Base = treeSource{Snapshot: base.String()}
	}
	for _, e := range edits {
		if e.data != nil {
			sum := sha256.Sum256([]byte(*e.data))
			req.Contents = append(req.Contents, writeContent{Length: int64(len(*e.data)), SHA256: hex.EncodeToString(sum[:]), Data: []byte(*e.data)})
			i := len(req.Contents) - 1
			e.Content = &i
		}
		req.Edits = append(req.Edits, e.writeEdit)
	}
	return req
}

// write posts a request, requires success and an independent verification.
func (f *swFixture) write(req treeWriteRequest) treeWriteResponse {
	f.t.Helper()
	r := f.post("/tree-write", req)
	if r.code != http.StatusOK {
		f.t.Fatalf("tree-write answered %d: %s", r.code, r.body)
	}
	f.verifyOK(req, r.resp)
	return r.resp
}

func (f *swFixture) edit(base restic.ID, edits ...testEdit) treeWriteResponse {
	f.t.Helper()
	return f.write(f.request(base, edits...))
}

func (f *swFixture) verify(req treeWriteRequest, resp treeWriteResponse) verifyWriteResponse {
	f.t.Helper()
	req.Contents = append([]writeContent(nil), req.Contents...)
	for i := range req.Contents {
		req.Contents[i].Data = nil
	}
	body, err := json.Marshal(verifyWriteRequest{Version: treeWriteVersion, Request: req, Result: resp})
	rtest.OK(f.t, err)
	w := serveReadRequest(f.srv, http.MethodPost, "/verify-write", string(body))
	if w.Code != http.StatusOK {
		f.t.Fatalf("verify-write answered %d: %s", w.Code, w.Body.String())
	}
	var out verifyWriteResponse
	rtest.OK(f.t, json.Unmarshal(w.Body.Bytes(), &out))
	return out
}

func (f *swFixture) verifyOK(req treeWriteRequest, resp treeWriteResponse) {
	f.t.Helper()
	if v := f.verify(req, resp); !v.OK {
		f.t.Fatalf("verify-write refused a fresh result: %s: %s", v.Code, v.Detail)
	}
}

func mustID(t *testing.T, s string) restic.ID {
	t.Helper()
	id, err := restic.ParseID(s)
	rtest.OK(t, err)
	return id
}

// flatten returns every node of a snapshot by relative path.
func (f *swFixture) flatten(id string) map[string]*data.Node {
	f.t.Helper()
	sn, err := data.LoadSnapshot(context.TODO(), f.repo, mustID(f.t, id))
	rtest.OK(f.t, err)
	rtest.OK(f.t, f.repo.LoadIndex(context.TODO(), nil))
	out := map[string]*data.Node{}
	var walk func(prefix string, tree restic.ID)
	walk = func(prefix string, tree restic.ID) {
		nodes, err := loadTestNodes(f.repo, tree)
		rtest.OK(f.t, err)
		for _, n := range nodes {
			p := path.Join(prefix, n.Name)
			out[p] = n
			if n.Type == data.NodeTypeDir {
				walk(p, *n.Subtree)
			}
		}
	}
	walk("", *sn.Tree)
	return out
}

func loadTestNodes(repo restic.BlobLoader, id restic.ID) ([]*data.Node, error) {
	tree, err := data.LoadTree(context.TODO(), repo, id)
	if err != nil {
		return nil, err
	}
	var nodes []*data.Node
	for item := range tree {
		if item.Error != nil {
			return nil, item.Error
		}
		nodes = append(nodes, item.Node)
	}
	return nodes, nil
}

func linkGroupsOf(nodes map[string]*data.Node) []string {
	groups := map[inodeKey][]string{}
	for p, n := range nodes {
		if n.Type == data.NodeTypeFile && n.Links > 1 {
			k := inodeKey{n.DeviceID, n.Inode}
			groups[k] = append(groups[k], p)
		}
	}
	var out []string
	for _, g := range groups {
		sort.Strings(g)
		out = append(out, strings.Join(g, ","))
	}
	sort.Strings(out)
	return out
}

// compare checks that two trees hold the same names, bytes (content blob IDs),
// modes, owners, link counts, symlink targets, xattrs and hard-link groups.
// Inode numbers, device IDs, times and owner names are not compared.
func (f *swFixture) compare(got, want map[string]*data.Node) {
	f.t.Helper()
	for p, w := range want {
		g := got[p]
		if g == nil {
			f.t.Errorf("%s missing", p)
			continue
		}
		type fields struct {
			Type               data.NodeType
			Mode               os.FileMode
			Size, Links        uint64
			UID, GID           uint32
			LinkTarget         string
			Content            restic.IDs
			ExtendedAttributes []data.ExtendedAttribute
		}
		gf := fields{g.Type, g.Mode, g.Size, g.Links, g.UID, g.GID, g.LinkTarget, g.Content, g.ExtendedAttributes}
		wf := fields{w.Type, w.Mode, w.Size, w.Links, w.UID, w.GID, w.LinkTarget, w.Content, w.ExtendedAttributes}
		if !reflect.DeepEqual(gf, wf) {
			f.t.Errorf("%s differs:\n got  %+v\n want %+v", p, gf, wf)
		}
	}
	for p := range got {
		if want[p] == nil {
			f.t.Errorf("%s unexpected", p)
		}
	}
	rtest.Equals(f.t, linkGroupsOf(want), linkGroupsOf(got))
	f.checkInodes(got)
}

// checkInodes requires that names sharing an inode form exactly one complete
// hard-link group: a synthesized inode never collides with another file.
func (f *swFixture) checkInodes(nodes map[string]*data.Node) {
	f.t.Helper()
	byInode := map[uint64][]*data.Node{}
	for _, n := range nodes {
		byInode[n.Inode] = append(byInode[n.Inode], n)
	}
	for ino, ns := range byInode {
		if len(ns) == 1 {
			continue
		}
		for _, n := range ns {
			if n.Type != data.NodeTypeFile || n.Links != uint64(len(ns)) || !reflect.DeepEqual(n.Content, ns[0].Content) || n.ChangeTime != ns[0].ChangeTime {
				f.t.Errorf("inode %d shared by %d names that are not one hard-link group", ino, len(ns))
				break
			}
		}
	}
}

// preserved requires byte-identical JSON for every node outside changed; a
// directory in dirs may differ only in its subtree.
func (f *swFixture) preserved(got, base map[string]*data.Node, changed, dirs []string) {
	f.t.Helper()
	skip := map[string]bool{}
	for _, p := range changed {
		skip[p] = true
	}
	subtree := map[string]bool{}
	for _, p := range dirs {
		subtree[p] = true
	}
	for p, b := range base {
		g := got[p]
		if skip[p] || g == nil {
			continue
		}
		gc, bc := *g, *b
		if subtree[p] {
			gc.Subtree, bc.Subtree = nil, nil
		}
		gj, _ := json.Marshal(&gc)
		bj, _ := json.Marshal(&bc)
		if string(gj) != string(bj) {
			f.t.Errorf("%s not preserved:\n got  %s\n base %s", p, gj, bj)
		}
	}
}

func (f *swFixture) twinMatches(resp treeWriteResponse) {
	f.t.Helper()
	head, pub := f.flatten(resp.Head.Snapshot), f.flatten(resp.Public.Snapshot)
	want := map[string]*data.Node{}
	for p, n := range head {
		private := false
		for _, part := range strings.Split(p, "/") {
			private = private || f.srv.w.excluded(part)
		}
		if !private {
			want[p] = n
		}
	}
	rtest.Equals(f.t, len(want), len(pub))
	f.preserved(pub, want, nil, func() []string {
		var dirs []string
		for p, n := range want {
			if n.Type == data.NodeTypeDir {
				dirs = append(dirs, p)
			}
		}
		return dirs
	}())
	rtest.Equals(f.t, uint64(len(pub)), resp.Public.Entries)
}

func (f *swFixture) snapshotTags(id string) []string {
	f.t.Helper()
	sn, err := data.LoadSnapshot(context.TODO(), f.repo, mustID(f.t, id))
	rtest.OK(f.t, err)
	return sn.Tags
}

func TestServeWriteOverwriteRenameAndTwin(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	baseNodes := f.flatten(base.String())

	req := f.request(base, wr("a/b/file2", "new bytes"))
	resp := f.write(req)
	got := f.flatten(resp.Head.Snapshot)
	rtest.Equals(t, etagOf(got["a/b/file2"]), resp.Edits[0].ETag)
	rtest.Equals(t, etagOf(baseNodes["a/b/file2"]), resp.Edits[0].BeforeETag)
	rtest.Equals(t, uint64(len(got)), resp.Head.Entries)
	rtest.Equals(t, []string{"plori-op:op-1", "plori-attempt:1"}, f.snapshotTags(resp.Head.Snapshot))
	rtest.Assert(t, resp.DataAdded > 0 && resp.DataAddedPacked > 0, "no added data recorded: %+v", resp)
	// A repeated request writes the same trees.
	again := f.write(req)
	rtest.Equals(t, resp.Head.Tree, again.Head.Tree)

	rtest.OK(t, os.WriteFile(filepath.Join(f.dir, "a/b/.plori-op-1"), []byte("new bytes"), 0600))
	rtest.OK(t, os.Chmod(filepath.Join(f.dir, "a/b/.plori-op-1"), 0644))
	rtest.OK(t, os.Rename(filepath.Join(f.dir, "a/b/.plori-op-1"), filepath.Join(f.dir, "a/b/file2")))
	ref := f.backup(&base, false)
	f.compare(got, f.flatten(ref.String()))
	f.preserved(got, baseNodes, []string{"a/b/file2", "a/b"}, []string{"a"})
	if got["a/b/file2"].Inode <= 0 || got["a/b/file2"].Inode == baseNodes["a/b/file2"].Inode {
		t.Errorf("overwrite kept inode %d", got["a/b/file2"].Inode)
	}
	// .plori-trash/old shares an inode with a/x: the twin is refused.
	rtest.Equals(t, []string{"/a/x"}, resp.IncompleteLinkGroups)
	rtest.Assert(t, resp.Public == nil, "twin written for an incomplete group")

	head1 := mustID(t, resp.Head.Snapshot)
	resp = f.edit(head1, ed(writeEdit{Op: "rename", Path: "a/b/file2", To: "n/m/moved"}))
	got2 := f.flatten(resp.Head.Snapshot)
	rtest.Equals(t, "n/m/moved", resp.Edits[0].Path)
	for _, d := range []string{"n", "n/m"} {
		rtest.OK(t, os.Mkdir(filepath.Join(f.dir, d), 0755))
		rtest.OK(t, os.Chmod(filepath.Join(f.dir, d), 0755))
	}
	rtest.OK(t, os.Rename(filepath.Join(f.dir, "a/b/file2"), filepath.Join(f.dir, "n/m/moved")))
	ref2 := f.backup(&ref, false)
	f.compare(got2, f.flatten(ref2.String()))
	rtest.Equals(t, got["a/b/file2"].Inode, got2["n/m/moved"].Inode)
	rtest.Assert(t, got2["n/m/moved"].ChangeTime.After(got["a/b/file2"].ChangeTime), "rename kept ctime")
	f.preserved(got2, got, []string{"a/b", "n", "n/m", "n/m/moved"}, []string{"a"})
	checker.TestCheckRepo(t, f.repo)
}

func TestServeWriteHardLinkAliases(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	baseNodes := f.flatten(base.String())

	// a/x has its second name in the trash; both names change.
	resp := f.edit(base, wr("a/x", "rewritten in place"), wr("a/b/link1", "both"))
	got := f.flatten(resp.Head.Snapshot)
	for _, p := range []string{"a/x", "a/b/link1"} {
		h, err := os.OpenFile(filepath.Join(f.dir, p), os.O_WRONLY|os.O_TRUNC, 0)
		rtest.OK(t, err)
		_, err = h.WriteString(map[string]string{"a/x": "rewritten in place", "a/b/link1": "both"}[p])
		rtest.OK(t, err)
		rtest.OK(t, h.Close())
	}
	ref := f.backup(&base, false)
	f.compare(got, f.flatten(ref.String()))
	rtest.Equals(t, got["a/x"].Content, got[".plori-trash/old"].Content)
	rtest.Equals(t, baseNodes["a/x"].Inode, got[".plori-trash/old"].Inode)
	rtest.Equals(t, got["a/b/file1"].Content, got["a/b/link1"].Content)
	f.preserved(got, baseNodes, []string{"a/x", ".plori-trash/old", "a/b/file1", "a/b/link1"}, []string{"a", "a/b", ".plori-trash"})

	// Trash one name of the public pair: the group now has a trash name.
	head := mustID(t, resp.Head.Snapshot)
	etag := etagOf(got["a/b/file1"])
	resp = f.edit(head, ed(writeEdit{Op: "trash", Path: "a/b/file1", Handle: "op-7", ExpectedETag: &etag}))
	rtest.Equals(t, uint64(1), resp.Edits[0].Deleted)
	got2 := f.flatten(resp.Head.Snapshot)
	rtest.OK(t, os.Rename(filepath.Join(f.dir, "a/b/file1"), filepath.Join(f.dir, ".plori-trash/op-7")))
	ref2 := f.backup(&ref, false)
	f.compare(got2, f.flatten(ref2.String()))
	rtest.Equals(t, []string{"/a/b/link1", "/a/x"}, resp.IncompleteLinkGroups)

	// Restore it, then trash a/x: the remaining public names form complete
	// groups and the twin is written.
	head = mustID(t, resp.Head.Snapshot)
	ax := etagOf(got2["a/x"])
	resp = f.edit(head, ed(writeEdit{Op: "restore", Path: "a/b/file1", Handle: "op-7"}),
		ed(writeEdit{Op: "trash", Path: "a/x", Handle: "op-8", ExpectedETag: &ax}))
	rtest.Equals(t, "a/b/file1", resp.Edits[0].Path)
	rtest.Equals(t, 0, len(resp.IncompleteLinkGroups))
	rtest.Assert(t, resp.Public != nil && !resp.Public.Empty, "twin missing")
	rtest.Equals(t, []string{"plori-op:op-1", "plori-attempt:1", "plori-public"}, f.snapshotTags(resp.Public.Snapshot))
	f.twinMatches(resp)
	checker.TestCheckRepo(t, f.repo)
}

// Empty-trash removes the trash directory; hard-linked files lose the trash
// names, as unlink(2) does.
func TestServeWriteEmptyTrash(t *testing.T) {
	f := newSWFixture(t)
	rtest.OK(t, os.Link(filepath.Join(f.dir, "a/b/file1"), filepath.Join(f.dir, ".plori-trash/third")))
	swWrite(t, filepath.Join(f.dir, ".plori-trash/dir/inner"), "inner", 0644)
	base := f.backup(nil, false)
	resp := f.edit(base, ed(writeEdit{Op: "empty-trash"}))
	rtest.Equals(t, uint64(3), resp.Edits[0].Deleted)
	got := f.flatten(resp.Head.Snapshot)
	rtest.OK(t, os.RemoveAll(filepath.Join(f.dir, ".plori-trash")))
	ref := f.backup(&base, false)
	refNodes := f.flatten(ref.String())
	f.compare(got, refNodes)
	rtest.Equals(t, uint64(1), got["a/x"].Links)
	rtest.Equals(t, uint64(0), got["a/x"].DeviceID)
	rtest.Equals(t, uint64(2), got["a/b/file1"].Links)
	rtest.Assert(t, resp.Public != nil && !resp.Public.Empty, "twin missing after the trash is gone")
	f.twinMatches(resp)
	// Emptying a missing trash is a no-op edit.
	resp = f.edit(mustID(t, resp.Head.Snapshot), ed(writeEdit{Op: "empty-trash"}))
	rtest.Equals(t, uint64(0), resp.Edits[0].Deleted)
	checker.TestCheckRepo(t, f.repo)
}

// An empty base, a private-only head with an empty twin, an empty merge
// result, and a write that leaves an empty tree.
func TestServeWriteEmptyTrees(t *testing.T) {
	f := newSWFixture(t)
	resp := f.edit(restic.ID{}, wr(".forge/state", "private"))
	rtest.Assert(t, !resp.Head.Empty && resp.Public != nil && resp.Public.Empty, "private-only head: %+v", resp)
	sn, err := data.LoadSnapshot(context.TODO(), f.repo, mustID(t, resp.Head.Snapshot))
	rtest.OK(t, err)
	rtest.Equals(t, []string{"/scan"}, sn.Paths)
	rtest.Assert(t, sn.Parent == nil, "parent on an empty base")

	resp = f.edit(restic.ID{}, ed(writeEdit{Op: "mkdir"}))
	rtest.Assert(t, resp.Head.Empty && resp.Public != nil && resp.Public.Empty && resp.Head.Snapshot == "", "empty result: %+v", resp)

	// A merge plan without entries writes the empty tree.
	merged := f.request(mustID(t, f.edit(restic.ID{}, wr("y", "y")).Head.Snapshot))
	merged.Edits, merged.Merge = nil, &mergePlan{}
	resp = f.write(merged)
	rtest.Assert(t, resp.Head.Empty && resp.Public != nil && resp.Public.Empty, "empty merge: %+v", resp)

	pub := f.edit(restic.ID{}, wr("x", "public"))
	head := mustID(t, pub.Head.Snapshot)
	x := etagOf(f.flatten(pub.Head.Snapshot)["x"])
	resp = f.edit(head, ed(writeEdit{Op: "trash", Path: "x", Handle: "h", ExpectedETag: &x}), ed(writeEdit{Op: "empty-trash"}))
	rtest.Assert(t, resp.Head.Empty && resp.Head.Snapshot == "", "emptied tree: %+v", resp)
}

func TestServeWriteMkdirChmodAndPublicTwin(t *testing.T) {
	f := newSWFixture(t)
	rtest.OK(t, os.Remove(filepath.Join(f.dir, ".plori-trash/old"))) // a/x keeps one name
	base := f.backup(nil, false)
	baseNodes := f.flatten(base.String())
	w := wr("p/q/new", "created")
	w.Mode = 0640
	resp := f.edit(base, ed(writeEdit{Op: "mkdir", Path: "p/q", Mode: 0750}), ed(writeEdit{Op: "chmod", Path: "a/mode", Mode: 0600}), w)
	got := f.flatten(resp.Head.Snapshot)
	rtest.OK(t, os.Mkdir(filepath.Join(f.dir, "p"), 0755))
	rtest.OK(t, os.Chmod(filepath.Join(f.dir, "p"), 0755))
	rtest.OK(t, os.Mkdir(filepath.Join(f.dir, "p/q"), 0750))
	rtest.OK(t, os.Chmod(filepath.Join(f.dir, "p/q"), 0750))
	rtest.OK(t, os.Chmod(filepath.Join(f.dir, "a/mode"), 0600))
	swWrite(t, filepath.Join(f.dir, "p/q/new"), "created", 0640)
	ref := f.backup(&base, false)
	f.compare(got, f.flatten(ref.String()))
	rtest.Equals(t, baseNodes["a/mode"].ExtendedAttributes, got["a/mode"].ExtendedAttributes)
	f.preserved(got, baseNodes, []string{"a/mode", "a"}, nil)
	rtest.Assert(t, resp.Public != nil && !resp.Public.Empty, "twin missing")
	f.twinMatches(resp)
	// The twin equals an archiver backup that leaves out the private names.
	pubRef := f.backup(nil, true)
	f.compare(f.flatten(resp.Public.Snapshot), f.flatten(pubRef.String()))
	_, ok := f.flatten(resp.Public.Snapshot)[".forge"]
	rtest.Assert(t, !ok, "private name in the twin")
	checker.TestCheckRepo(t, f.repo)
}

func (f *swFixture) countFiles(tpe restic.FileType) int {
	f.t.Helper()
	n := 0
	rtest.OK(f.t, f.repo.List(context.TODO(), tpe, func(restic.ID, int64) error { n++; return nil }))
	return n
}

func TestServeWriteRefusals(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	before := f.countFiles(restic.SnapshotFile)
	wrong := uint64(42)
	zero := uint64(0)
	withETag := func(e testEdit, etag *uint64) testEdit { e.ExpectedETag = etag; return e }
	cases := []struct {
		edit testEdit
		code string
	}{
		{withETag(wr("a/b/file2", "x"), &wrong), codeStale},
		{withETag(wr("a/b/file2", "x"), &zero), codeStale},
		{wr("a/sym/x", "x"), codeRefused},
		{wr("a/x/y", "x"), codeNotDir},
		{wr("a/b", "x"), codeRefused},
		{ed(writeEdit{Op: "rename", Path: "a/b/file2", To: "a/x"}), codeExists},
		{ed(writeEdit{Op: "rename", Path: "a/nothing", To: "a/y"}), codeNotFound},
		{ed(writeEdit{Op: "rename", Path: "a/nothing", To: "a/y", ExpectedETag: &wrong}), codeStale},
		{ed(writeEdit{Op: "trash", Path: "a/sym", Handle: "t"}), codeRefused},
		{ed(writeEdit{Op: "mkdir", Path: "a/x"}), codeExists},
		{ed(writeEdit{Op: "restore", Path: "a/r", Handle: "missing"}), codeNotFound},
		{ed(writeEdit{Op: "restore", Path: "a/x", Handle: "old"}), codeExists},
	}
	for _, c := range cases {
		r := f.post("/tree-write", f.request(base, ed(writeEdit{Op: "mkdir", Path: "ok"}), c.edit))
		rtest.Equals(t, http.StatusConflict, r.code)
		rtest.Equals(t, c.code, r.refusal.Code)
		rtest.Equals(t, 1, r.refusal.Edit)
	}
	r := f.post("/tree-write", f.request(base, withETag(wr("a/b/file2", "x"), &wrong)))
	rtest.Equals(t, etagOf(f.flatten(base.String())["a/b/file2"]), r.refusal.CurrentETag)
	etag := etagOf(f.flatten(base.String())["a/b/file2"])
	f.edit(base, withETag(wr("a/b/file2", "x"), &etag))
	rtest.Equals(t, before+1, f.countFiles(restic.SnapshotFile)) // only the accepted edit wrote a head (no twin: trash alias)
}

// A malformed request is refused before anything is read: unknown fields,
// another protocol version, fields an op does not take, a content that does
// not match its descriptor, and bytes in a verify-write request.
func TestServeWriteRequestValidation(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	good := f.request(base, wr("a/new", "x"))
	body, err := json.Marshal(good)
	rtest.OK(t, err)
	for name, c := range map[string]struct {
		body string
		code string
	}{
		"unknown field":      {strings.Replace(string(body), `"version":1`, `"version":1,"extra":true`, 1), "invalid_request"},
		"unknown edit field": {strings.Replace(string(body), `"op":"write"`, `"op":"write","source":"/tmp/x"`, 1), "invalid_request"},
		"version":            {strings.Replace(string(body), `"version":1`, `"version":2`, 1), "unsupported_version"},
		"no owner":           {strings.Replace(string(body), fmt.Sprintf(`"owner":[%d,%d],`, f.owner[0], f.owner[1]), "", 1), "invalid_request"},
		"two bodies":         {string(body) + string(body), "invalid_request"},
	} {
		w := serveReadRequest(f.srv, http.MethodPost, "/tree-write", c.body)
		rtest.Assert(t, w.Code == http.StatusBadRequest && strings.Contains(w.Body.String(), c.code), "%s: %d %s", name, w.Code, w.Body.String())
	}
	bad := []treeWriteRequest{f.request(base, wr("a/new", "x")), f.request(base, ed(writeEdit{Op: "rename", Path: "a/x", To: "b", Mode: 0644})),
		f.request(base, ed(writeEdit{Op: "trash", Path: "a/x"})), f.request(base, wr("../x", "x")), f.request(base, wr("a/new", "x"))}
	bad[0].Contents[0].Data = []byte("y")
	bad[4].Base.Empty = true
	for i, req := range bad {
		r := f.post("/tree-write", req)
		rtest.Assert(t, r.code == http.StatusBadRequest, "case %d: %d %s", i, r.code, r.body)
	}
	vreq := verifyWriteRequest{Version: treeWriteVersion, Request: good, Result: treeWriteResponse{Head: treeRole{Empty: true}, Contents: []contentReceipt{{}}}}
	r := f.post("/verify-write", vreq)
	rtest.Assert(t, r.code == http.StatusBadRequest, "verify-write with data: %d %s", r.code, r.body)
	rtest.Equals(t, 0, f.countFiles(restic.LockFile))
	rtest.Equals(t, 1, f.countFiles(restic.SnapshotFile))

	w := serveReadRequest(f.srv, http.MethodGet, "/version", "")
	rtest.Equals(t, http.StatusOK, w.Code)
	var v versionResponse
	rtest.OK(t, json.Unmarshal(w.Body.Bytes(), &v))
	rtest.Equals(t, treeWriteVersion, v.Version)
	rtest.Equals(t, testTrash, v.TrashDir)
}

// A tree with a field this restic version does not know cannot be rewritten
// without loss. An edit in it is refused; an edit elsewhere keeps its tree ID
// in the head, and in the twin, which keeps a subtree without private names
// by ID and leaves out a private one without reading it.
func TestServeWriteUnknownFieldGuard(t *testing.T) {
	for _, parent := range []string{".forge", "pub"} {
		repo, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{})
		f := &swFixture{t: t, repo: repo, be: be, srv: newTestServeWriteServer(t, be), owner: [2]uint32{1, 1}, clock: time.Now()}
		ctx := context.TODO()
		var root, odd restic.ID
		rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) error {
			var err error
			odd, _, _, err = up.SaveBlob(ctx, restic.TreeBlob, []byte(`{"nodes":[{"name":"f","type":"file","mode":420,"uid":0,"gid":0,"content":[],"plori_future":1}]}`+"\n"), restic.ID{}, false)
			if err != nil {
				return err
			}
			b := data.NewTreeJSONBuilder()
			rtest.OK(t, b.AddNode(&data.Node{Name: parent, Type: data.NodeTypeDir, Mode: os.ModeDir | 0755, Subtree: &odd}))
			buf, _ := b.Finalize()
			root, _, _, err = up.SaveBlob(ctx, restic.TreeBlob, buf, restic.ID{}, false)
			return err
		}))
		sid, err := data.SaveSnapshot(ctx, repo, &data.Snapshot{Tree: &root, Paths: []string{"/scan"}, Time: time.Now()})
		rtest.OK(t, err)
		r := f.post("/tree-write", f.request(sid, wr(parent+"/g", "x")))
		rtest.Equals(t, http.StatusInternalServerError, r.code)
		resp := f.edit(sid, wr("top", "x"))
		rtest.OK(t, repo.LoadIndex(ctx, nil))
		nodes, err := loadTestNodes(repo, mustID(t, resp.Head.Tree))
		rtest.OK(t, err)
		rtest.Equals(t, odd, *nodes[0].Subtree)
		rtest.Assert(t, resp.Public != nil && !resp.Public.Empty, "twin missing")
		pub, err := loadTestNodes(repo, mustID(t, resp.Public.Tree))
		rtest.OK(t, err)
		if parent == "pub" {
			rtest.Equals(t, odd, *pub[0].Subtree)
		} else {
			rtest.Equals(t, 1, len(pub))
		}
	}
}

// entriesOf is resticrev.Read's manifest of a flattened tree: restic content
// tokens and link-group labels over all names.
func entriesOf(nodes map[string]*data.Node) []mergeEntry {
	groups := map[inodeKey][]string{}
	for p, n := range nodes {
		if n.Type == data.NodeTypeFile && n.Links > 1 {
			groups[inodeKey{n.DeviceID, n.Inode}] = append(groups[inodeKey{n.DeviceID, n.Inode}], p)
		}
	}
	var out []mergeEntry
	for p, n := range nodes {
		e := mergeEntry{Path: p, Kind: string(n.Type), Mode: uint32(n.Mode.Perm())}
		if n.Mode&os.ModeSetuid != 0 {
			e.Mode |= 04000
		}
		switch n.Type {
		case data.NodeTypeFile:
			e.Size, e.Digest = int64(n.Size), contentToken(n.Content)
			if n.Links > 1 {
				e.LinkGroup = groupToken(groups[inodeKey{n.DeviceID, n.Inode}])
			}
		case data.NodeTypeSymlink:
			e.Target, e.Size = n.LinkTarget, int64(len(n.LinkTarget))
		}
		out = append(out, e)
	}
	return out
}

func TestServeWriteMergeEntries(t *testing.T) {
	f := newSWFixture(t)
	rtest.OK(t, os.Remove(filepath.Join(f.dir, ".plori-trash/old")))
	current := f.backup(nil, false)
	currentNodes := f.flatten(current.String())
	// The incoming side: a changed file, a new hard-link pair, a removed file
	// and a mode change, saved as its own snapshot (a source).
	incomingDir := t.TempDir()
	swWrite(t, filepath.Join(incomingDir, "c/d/e/deep"), "incoming deep", 0644)
	swWrite(t, filepath.Join(incomingDir, "new/pair"), "pair", 0644)
	rtest.OK(t, os.Chmod(filepath.Join(incomingDir, "new"), 0755))
	rtest.OK(t, os.Link(filepath.Join(incomingDir, "new/pair"), filepath.Join(incomingDir, "new/pair2")))
	other := &swFixture{t: t, dir: incomingDir, repo: f.repo, srv: f.srv, owner: f.owner}
	incoming := other.backup(nil, false)
	incomingNodes := other.flatten(incoming.String())

	// The merge result: current's public tree with incoming's deep file and
	// pair, a/b/file2 removed, a/mode 0600, and a generated diff3 file.
	merged := map[string]*data.Node{}
	for p, n := range currentNodes {
		if !strings.HasPrefix(p, ".forge") && !strings.HasPrefix(p, ".plori-trash") && p != "a/b/file2" {
			merged[p] = n
		}
	}
	for _, p := range []string{"c/d/e/deep", "new", "new/pair", "new/pair2"} {
		merged[p] = incomingNodes[p]
	}
	entries := entriesOf(merged)
	diff3 := "<<<<<<< current\nA\n=======\nB\n>>>>>>> incoming\n"
	sum := sha256.Sum256([]byte(diff3))
	for i := range entries {
		if entries[i].Path == "a/mode" {
			entries[i].Mode = 0600
		}
	}
	entries = append(entries, mergeEntry{Path: "a/conflict.txt", Kind: "file", Mode: 0644, Size: int64(len(diff3)), Digest: hex.EncodeToString(sum[:])})
	req := f.request(current)
	req.Edits = nil
	req.Merge = &mergePlan{Sources: []treeSource{{Snapshot: incoming.String()}, {Empty: true}}, Entries: entries}
	req.Contents = []writeContent{{Length: int64(len(diff3)), SHA256: hex.EncodeToString(sum[:]), Data: []byte(diff3)}}
	resp := f.write(req)
	got := f.flatten(resp.Head.Snapshot)

	// Reference: materialize the same tree as the helper does and back it up.
	rtest.OK(t, os.RemoveAll(filepath.Join(f.dir, ".forge")))
	rtest.OK(t, os.RemoveAll(filepath.Join(f.dir, ".plori-trash")))
	rtest.OK(t, os.Remove(filepath.Join(f.dir, "a/b/file2")))
	rtest.OK(t, os.Chmod(filepath.Join(f.dir, "a/mode"), 0600))
	swWrite(t, filepath.Join(f.dir, "c/d/e/deep"), "incoming deep", 0644)
	swWrite(t, filepath.Join(f.dir, "new/pair"), "pair", 0644)
	rtest.OK(t, os.Chmod(filepath.Join(f.dir, "new"), 0755))
	rtest.OK(t, os.Link(filepath.Join(f.dir, "new/pair"), filepath.Join(f.dir, "new/pair2")))
	swWrite(t, filepath.Join(f.dir, "a/conflict.txt"), diff3, 0644)
	ref := f.backup(&current, false)
	f.compare(got, f.flatten(ref.String()))
	// Retained names keep their native nodes byte for byte.
	f.preserved(got, currentNodes, []string{"a", "a/b", "a/mode", "c/d/e/deep", "c/d/e", "c", "c/d"}, nil)
	rtest.Assert(t, resp.Public != nil && resp.Public.Tree == resp.Head.Tree, "twin of a tree without private names differs")

	// A conflicted comparison still writes the pair: conflicted paths keep
	// current's node (here a/b/file2, which incoming also changed), clean
	// changes apply (incoming's deep file).
	conflicted := map[string]*data.Node{}
	for p, n := range currentNodes {
		if !strings.HasPrefix(p, ".forge") && !strings.HasPrefix(p, ".plori-trash") {
			conflicted[p] = n
		}
	}
	conflicted["c/d/e/deep"] = incomingNodes["c/d/e/deep"]
	creq := f.request(current)
	creq.Edits = nil
	creq.Merge = &mergePlan{Sources: []treeSource{{Snapshot: incoming.String()}}, Entries: entriesOf(conflicted)}
	cresp := f.write(creq)
	rtest.Assert(t, cresp.Public != nil && cresp.Public.Tree == cresp.Head.Tree, "conflicted merge twin: %+v", cresp.Public)
	cgot := f.flatten(cresp.Head.Snapshot)
	rtest.Equals(t, incomingNodes["c/d/e/deep"].Content, cgot["c/d/e/deep"].Content)
	f.preserved(cgot, currentNodes, []string{"c", "c/d", "c/d/e", "c/d/e/deep"}, nil)

	// A plan that names content no source holds is refused before writing.
	snapshots := f.countFiles(restic.SnapshotFile)
	req2 := f.request(current)
	req2.Edits = nil
	req2.Merge = &mergePlan{Entries: []mergeEntry{{Path: "f", Kind: "file", Mode: 0644, Size: 3, Digest: contentToken(restic.IDs{restic.NewRandomID()})}}}
	r := f.post("/tree-write", req2)
	rtest.Equals(t, http.StatusBadRequest, r.code)
	rtest.Equals(t, snapshots, f.countFiles(restic.SnapshotFile))
	checker.TestCheckRepo(t, f.repo)
}
