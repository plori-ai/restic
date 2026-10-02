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
	"github.com/restic/restic/internal/checker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// swFixture is a real directory backed up with the archiver (the helper's
// `restic backup .`), the repository and a serve-write handler on it. The same
// edits are applied to the directory with the helper's POSIX operations
// (workspacehelper/mutation.go) and backed up again as the reference.
type swFixture struct {
	t     *testing.T
	dir   string
	repo  *repository.Repository
	h     *serveWriteHandler
	owner [2]uint32
}

func swWrite(t *testing.T, p, content string, mode os.FileMode) {
	t.Helper()
	rtest.OK(t, os.MkdirAll(filepath.Dir(p), 0755))
	rtest.OK(t, os.WriteFile(p, []byte(content), 0600))
	rtest.OK(t, os.Chmod(p, mode))
}

func newSWFixture(t *testing.T) *swFixture {
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
	repo := repository.TestRepository(t)
	return &swFixture{t: t, dir: dir, repo: repo, h: newServeWriteHandler(repo, defaultPublicExcludes()),
		owner: [2]uint32{uint32(os.Getuid()), uint32(os.Getgid())}}
}

func (f *swFixture) backup(parent *restic.ID, public bool) restic.ID {
	f.t.Helper()
	back := rtest.Chdir(f.t, f.dir)
	defer back()
	arch := archiver.New(f.repo, fs.Local{}, archiver.Options{})
	if public {
		arch.SelectByName = func(item string) bool { return !f.h.excluded(filepath.Base(item)) || item == f.dir }
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

func (f *swFixture) post(uri string, req any) (int, writeResponse, writeRefusal) {
	f.t.Helper()
	body, err := json.Marshal(req)
	rtest.OK(f.t, err)
	w := serveReadRequest(f.h, http.MethodPost, uri, string(body))
	var resp writeResponse
	var refusal writeRefusal
	switch w.Code {
	case http.StatusOK:
		rtest.OK(f.t, json.Unmarshal(w.Body.Bytes(), &resp))
	case http.StatusConflict:
		rtest.OK(f.t, json.Unmarshal(w.Body.Bytes(), &refusal))
	default:
		f.t.Logf("%s answered %d: %s", uri, w.Code, w.Body.String())
	}
	return w.Code, resp, refusal
}

func (f *swFixture) edit(base restic.ID, edits ...writeEdit) writeResponse {
	f.t.Helper()
	code, resp, _ := f.post("/edit", writeRequest{Base: base.String(), OpID: "op-1", CopyID: "copy-1", Owner: &f.owner, Edits: edits})
	rtest.Equals(f.t, http.StatusOK, code)
	return resp
}

// flatten returns every node of a snapshot by relative path.
func (f *swFixture) flatten(id string) map[string]*data.Node {
	f.t.Helper()
	sid, err := restic.ParseID(id)
	rtest.OK(f.t, err)
	sn, err := data.LoadSnapshot(context.TODO(), f.repo, sid)
	rtest.OK(f.t, err)
	out := map[string]*data.Node{}
	var walk func(prefix string, tree restic.ID)
	walk = func(prefix string, tree restic.ID) {
		nodes, err := f.h.loadNodes(context.TODO(), tree, true)
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

func (f *swFixture) twinMatches(resp writeResponse) {
	f.t.Helper()
	head, pub := f.flatten(resp.Head.Snapshot), f.flatten(resp.Public.Snapshot)
	want := map[string]*data.Node{}
	for p, n := range head {
		private := false
		for _, part := range strings.Split(p, "/") {
			private = private || f.h.excluded(part)
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

func TestServeWriteOverwriteRenameAndTwin(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	baseNodes := f.flatten(base.String())

	resp := f.edit(base, writeEdit{Op: "write", Path: "a/b/file2", Data: []byte("new bytes")})
	got := f.flatten(resp.Head.Snapshot)
	rtest.Equals(t, etagOf(got["a/b/file2"]), resp.Edits[0].ETag)
	rtest.Equals(t, uint64(len(got)), resp.Head.Entries)

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

	head1, _ := restic.ParseID(resp.Head.Snapshot)
	resp = f.edit(head1, writeEdit{Op: "rename", Path: "a/b/file2", To: "n/m/moved"})
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
	resp := f.edit(base, writeEdit{Op: "write", Path: "a/x", Data: []byte("rewritten in place")},
		writeEdit{Op: "write", Path: "a/b/link1", Data: []byte("both")})
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

	// Delete one name of the public pair: the group now has a trash name.
	head, _ := restic.ParseID(resp.Head.Snapshot)
	resp = f.edit(head, writeEdit{Op: "delete", Path: "a/b/file1", Trash: "op-7"})
	got2 := f.flatten(resp.Head.Snapshot)
	rtest.OK(t, os.Rename(filepath.Join(f.dir, "a/b/file1"), filepath.Join(f.dir, ".plori-trash/op-7")))
	ref2 := f.backup(&ref, false)
	f.compare(got2, f.flatten(ref2.String()))
	rtest.Equals(t, []string{"/a/b/link1", "/a/x"}, resp.IncompleteLinkGroups)

	// Restore it; then empty both private aliases' groups of public names by
	// renaming a/x into the trash: the remaining public names form complete
	// groups and the twin is written.
	head, _ = restic.ParseID(resp.Head.Snapshot)
	resp = f.edit(head, writeEdit{Op: "rename", Path: ".plori-trash/op-7", To: "a/b/file1"},
		writeEdit{Op: "delete", Path: "a/x", Trash: "op-8"})
	rtest.Equals(t, 0, len(resp.IncompleteLinkGroups))
	rtest.Assert(t, resp.Public != nil, "twin missing")
	f.twinMatches(resp)
	checker.TestCheckRepo(t, f.repo)
}

func TestServeWriteMkdirChmodAndPublicTwin(t *testing.T) {
	f := newSWFixture(t)
	rtest.OK(t, os.Remove(filepath.Join(f.dir, ".plori-trash/old"))) // a/x keeps one name
	base := f.backup(nil, false)
	baseNodes := f.flatten(base.String())
	resp := f.edit(base, writeEdit{Op: "mkdir", Path: "p/q", Mode: 0750}, writeEdit{Op: "chmod", Path: "a/mode", Mode: 0600},
		writeEdit{Op: "write", Path: "p/q/new", Data: []byte("created"), Mode: 0640})
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
	rtest.Assert(t, resp.Public != nil, "twin missing")
	f.twinMatches(resp)
	// The twin equals an archiver backup that leaves out the private names.
	pubRef := f.backup(nil, true)
	f.compare(f.flatten(resp.Public.Snapshot), f.flatten(pubRef.String()))
	_, ok := f.flatten(resp.Public.Snapshot)[".forge"]
	rtest.Assert(t, !ok, "private name in the twin")
	checker.TestCheckRepo(t, f.repo)
}

func TestServeWriteRefusals(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	before := 0
	rtest.OK(t, f.repo.List(context.TODO(), restic.SnapshotFile, func(restic.ID, int64) error { before++; return nil }))
	wrong := uint64(42)
	zero := uint64(0)
	cases := []struct {
		edit writeEdit
		code string
	}{
		{writeEdit{Op: "write", Path: "a/b/file2", Data: []byte("x"), ExpectedETag: &wrong}, codeStale},
		{writeEdit{Op: "write", Path: "a/b/file2", Data: []byte("x"), ExpectedETag: &zero}, codeStale},
		{writeEdit{Op: "write", Path: "a/sym/x", Data: []byte("x")}, codeRefused},
		{writeEdit{Op: "write", Path: "a/x/y", Data: []byte("x")}, codeNotDir},
		{writeEdit{Op: "rename", Path: "a/b/file2", To: "a/x"}, codeExists},
		{writeEdit{Op: "rename", Path: "a/nothing", To: "a/y"}, codeNotFound},
		{writeEdit{Op: "delete", Path: "a/sym", Trash: "t"}, codeRefused},
		{writeEdit{Op: "mkdir", Path: "a/x"}, codeExists},
	}
	for _, c := range cases {
		code, _, refusal := f.post("/edit", writeRequest{Base: base.String(), OpID: "op", Edits: []writeEdit{{Op: "mkdir", Path: "ok"}, c.edit}})
		rtest.Equals(t, http.StatusConflict, code)
		rtest.Equals(t, c.code, refusal.Code)
		rtest.Equals(t, 1, refusal.Edit)
	}
	n := f.flatten(base.String())["a/b/file2"]
	etag := etagOf(n)
	code, _, _ := f.post("/edit", writeRequest{Base: base.String(), OpID: "op", Edits: []writeEdit{{Op: "write", Path: "a/b/file2", Data: []byte("x"), ExpectedETag: &etag}}})
	rtest.Equals(t, http.StatusOK, code)
	after := 0
	rtest.OK(t, f.repo.List(context.TODO(), restic.SnapshotFile, func(restic.ID, int64) error { after++; return nil }))
	rtest.Equals(t, before+1, after) // only the accepted edit wrote a head (no twin: trash alias)
}

// A tree with a field this restic version does not know cannot be rewritten
// without loss. An edit in it is refused; an edit elsewhere keeps its tree ID
// in the head, and in the twin, which keeps a subtree without private names
// by ID and leaves out a private one without reading it.
func TestServeWriteUnknownFieldGuard(t *testing.T) {
	for _, parent := range []string{".forge", "pub"} {
		repo := repository.TestRepository(t)
		h := newServeWriteHandler(repo, defaultPublicExcludes())
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
		w := serveReadRequest(h, http.MethodPost, "/edit", fmt.Sprintf(`{"base":%q,"op_id":"o","edits":[{"op":"write","path":"%s/g","data":"eA=="}]}`, sid, parent))
		rtest.Equals(t, http.StatusInternalServerError, w.Code)
		w = serveReadRequest(h, http.MethodPost, "/edit", fmt.Sprintf(`{"base":%q,"op_id":"o","edits":[{"op":"write","path":"top","data":"eA=="}]}`, sid))
		rtest.Equals(t, http.StatusOK, w.Code)
		var resp writeResponse
		rtest.OK(t, json.Unmarshal(w.Body.Bytes(), &resp))
		tid, _ := restic.ParseID(resp.Head.Tree)
		nodes, err := h.loadNodes(ctx, tid, false)
		rtest.OK(t, err)
		rtest.Equals(t, odd, *nodes[0].Subtree)
		rtest.Assert(t, resp.Public != nil, "twin missing")
		pid, _ := restic.ParseID(resp.Public.Tree)
		pub, err := h.loadNodes(ctx, pid, false)
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
	other := &swFixture{t: t, dir: incomingDir, repo: f.repo, h: f.h, owner: f.owner}
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
	diff3 := []byte("<<<<<<< current\nA\n=======\nB\n>>>>>>> incoming\n")
	sum := sha256.Sum256(diff3)
	for i := range entries {
		if entries[i].Path == "a/mode" {
			entries[i].Mode = 0600
		}
	}
	entries = append(entries, mergeEntry{Path: "a/conflict.txt", Kind: "file", Mode: 0644, Size: int64(len(diff3)), Digest: hex.EncodeToString(sum[:])})
	code, resp, _ := f.post("/merge-write", writeRequest{Base: current.String(), Sources: []string{incoming.String()}, OpID: "op-m", CopyID: "integration",
		Owner: &f.owner, Entries: entries, Blobs: map[string][]byte{hex.EncodeToString(sum[:]): diff3}})
	rtest.Equals(t, http.StatusOK, code)
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
	swWrite(t, filepath.Join(f.dir, "a/conflict.txt"), string(diff3), 0644)
	ref := f.backup(&current, false)
	f.compare(got, f.flatten(ref.String()))
	// Retained names keep their native nodes byte for byte.
	f.preserved(got, currentNodes, []string{"a", "a/b", "a/mode", "c/d/e/deep", "c/d/e", "c", "c/d"}, nil)
	rtest.Assert(t, resp.Public != nil && resp.Public.Tree == resp.Head.Tree, "twin of a tree without private names differs")
	checker.TestCheckRepo(t, f.repo)
}
