package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/restic/restic/internal/checker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// selectFrom returns the manifest entries of paths in a flattened source, each
// with a selector of role at the same path. Labels are computed over every
// name of the source, as resticrev.Read labels a revision.
func selectFrom(role string, src map[string]*data.Node, paths ...string) []mergeEntry {
	byPath := map[string]mergeEntry{}
	for _, e := range entriesOf(src) {
		byPath[e.Path] = e
	}
	out := make([]mergeEntry, 0, len(paths))
	for _, p := range paths {
		e, ok := byPath[p]
		if !ok {
			panic("no source entry " + p)
		}
		e.Source = &mergeSelector{Role: role, Path: p, LinkGroup: e.LinkGroup}
		out = append(out, e)
	}
	return out
}

// pathsOf lists the paths of a flattened tree that keep returns true for.
func pathsOf(nodes map[string]*data.Node, keep func(string) bool) []string {
	var out []string
	for p := range nodes {
		if keep(p) {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func generatedFile(p, content string, mode uint32) (mergeEntry, writeContent) {
	sum := sha256.Sum256([]byte(content))
	digest := hex.EncodeToString(sum[:])
	return mergeEntry{Path: p, Kind: "file", Mode: mode, Size: int64(len(content)), Digest: digest, Source: &mergeSelector{Role: roleGenerated}},
		writeContent{Length: int64(len(content)), SHA256: digest, Data: []byte(content)}
}

// selected builds a selected merge request on base with role sources.
func (f *swFixture) selected(base restic.ID, sources []treeSource, entries []mergeEntry, contents ...writeContent) treeWriteRequest {
	req := f.request(base)
	req.Edits = nil
	req.Merge = &mergePlan{Sources: sources, Entries: entries}
	req.Contents = contents
	return req
}

// sameNative requires that a written node is its selected source node byte
// for byte; a directory may differ in its subtree only.
func sameNative(t *testing.T, p string, got, want *data.Node) {
	t.Helper()
	g, w := *got, *want
	if g.Type == data.NodeTypeDir {
		g.Subtree, w.Subtree = nil, nil
	}
	gj, err := json.Marshal(&g)
	rtest.OK(t, err)
	wj, err := json.Marshal(&w)
	rtest.OK(t, err)
	if string(gj) != string(wj) {
		t.Errorf("%s is not its native node:\n got  %s\n want %s", p, gj, wj)
	}
}

func setXattr(t *testing.T, p, value string) bool {
	if err := syscall.Setxattr(p, "user.plori", []byte(value), 0); err != nil {
		t.Logf("no user xattrs: %v", err)
		return false
	}
	return true
}

// roleFixture is the standard fixture as current, plus a base and an incoming
// snapshot in the same repository.
type roleFixture struct {
	*swFixture
	current, base, incoming    restic.ID
	currentN, baseN, incomingN map[string]*data.Node
	sources                    []treeSource
	xattrs                     bool
}

func newRoleFixture(t *testing.T) *roleFixture {
	f := newSWFixture(t)
	r := &roleFixture{swFixture: f}
	baseDir := t.TempDir()
	swWrite(t, filepath.Join(baseDir, "bonly/f"), "joined bytes", 0640)
	swWrite(t, filepath.Join(baseDir, "a/b/file2"), "base two", 0644)
	r.xattrs = setXattr(t, filepath.Join(baseDir, "bonly/f"), "base")
	incomingDir := t.TempDir()
	swWrite(t, filepath.Join(incomingDir, "c/d/e/deep"), "incoming deep", 0600)
	swWrite(t, filepath.Join(incomingDir, "new/pair"), "pair", 0644)
	rtest.OK(t, os.Link(filepath.Join(incomingDir, "new/pair"), filepath.Join(incomingDir, "new/pair2")))
	rtest.OK(t, os.Link(filepath.Join(incomingDir, "new/pair"), filepath.Join(incomingDir, "new/pair3")))
	swWrite(t, filepath.Join(incomingDir, "inc/same"), "joined bytes", 0640)
	swWrite(t, filepath.Join(incomingDir, "inc/x"), "incoming x", 0644)
	setXattr(t, filepath.Join(incomingDir, "inc/x"), "incoming")
	for _, d := range []string{"bonly", "a", "a/b"} {
		rtest.OK(t, os.Chmod(filepath.Join(baseDir, d), 0750))
	}
	for _, d := range []string{"c", "c/d", "c/d/e", "new", "inc"} {
		rtest.OK(t, os.Chmod(filepath.Join(incomingDir, d), 0755))
	}
	r.current = f.backup(nil, false)
	r.base = (&swFixture{t: t, dir: baseDir, repo: f.repo, srv: f.srv, owner: f.owner}).backup(nil, false)
	r.incoming = (&swFixture{t: t, dir: incomingDir, repo: f.repo, srv: f.srv, owner: f.owner}).backup(nil, false)
	r.currentN, r.baseN, r.incomingN = f.flatten(r.current.String()), f.flatten(r.base.String()), f.flatten(r.incoming.String())
	r.sources = []treeSource{{Role: "base", Snapshot: r.base.String()}, {Role: "current", Snapshot: r.current.String()}, {Role: "incoming", Snapshot: r.incoming.String()}}
	return r
}

// A selected plan copies native nodes from each role byte for byte, keeps
// private current names in the head, and writes generated entries from the
// plan.
func TestServeWriteSelectedRolesAndPrivateNames(t *testing.T) {
	r := newRoleFixture(t)
	f := r.swFixture
	if r.xattrs {
		rtest.Assert(t, len(r.baseN["bonly/f"].ExtendedAttributes) > 0, "base xattr not recorded")
	}
	// Lossless: every current name, private ones included, except the
	// deep file, which incoming changed; base's bonly; incoming's group
	// and inc/x; a generated diff3 file and directory.
	entries := selectFrom("current", r.currentN, pathsOf(r.currentN, func(p string) bool { return p != "c/d/e/deep" })...)
	entries = append(entries, selectFrom("base", r.baseN, "bonly", "bonly/f")...)
	entries = append(entries, selectFrom("incoming", r.incomingN, "c/d/e/deep", "new", "new/pair", "new/pair2", "new/pair3", "inc", "inc/same", "inc/x")...)
	diff3, content := generatedFile("a/conflict.txt", "<<<<<<< current\nA\n=======\nB\n>>>>>>> incoming\n", 0640)
	entries = append(entries, diff3, mergeEntry{Path: "gen", Kind: "dir", Mode: 0700, Source: &mergeSelector{Role: roleGenerated}})
	req := f.selected(r.current, r.sources, entries, content)
	resp := f.write(req)
	got := f.flatten(resp.Head.Snapshot)
	rtest.Equals(t, len(entries), len(got))
	rtest.Equals(t, uint64(len(entries)), resp.Head.Entries)

	native := map[string]*data.Node{}
	for p, n := range r.currentN {
		native[p] = n
	}
	for _, p := range []string{"bonly", "bonly/f"} {
		native[p] = r.baseN[p]
	}
	for _, p := range []string{"c/d/e/deep", "new", "new/pair", "new/pair2", "new/pair3", "inc", "inc/same", "inc/x"} {
		native[p] = r.incomingN[p]
	}
	for p, n := range native {
		if p == "a" {
			continue // its names changed: checked below
		}
		sameNative(t, p, got[p], n)
	}
	// Every role keeps inode, ctime and xattrs; the private names are in
	// the head with their own owners.
	for _, c := range []struct {
		p    string
		want *data.Node
	}{{"bonly/f", r.baseN["bonly/f"]}, {".forge/state", r.currentN[".forge/state"]}, {"a/mode", r.currentN["a/mode"]}, {"inc/x", r.incomingN["inc/x"]}, {"new/pair2", r.incomingN["new/pair2"]}} {
		g := got[c.p]
		rtest.Equals(t, c.want.Inode, g.Inode)
		rtest.Equals(t, c.want.ChangeTime, g.ChangeTime)
		rtest.Equals(t, c.want.ExtendedAttributes, g.ExtendedAttributes)
		rtest.Equals(t, c.want.UID, g.UID)
	}
	rtest.Equals(t, *r.incomingN["new"].Subtree, *got["new"].Subtree)
	rtest.Equals(t, *r.baseN["bonly"].Subtree, *got["bonly"].Subtree)
	// a gained a name: its mtime and ctime are the request time.
	now, err := time.Parse(time.RFC3339Nano, req.Time)
	rtest.OK(t, err)
	rtest.Assert(t, got["a"].ModTime.Equal(now) && got["a"].ChangeTime.Equal(now), "a times: %v %v", got["a"].ModTime, got["a"].ChangeTime)
	rtest.Equals(t, r.currentN["a"].Inode, got["a"].Inode)
	// c/d/e kept its names: only its subtree changed.
	rtest.Assert(t, *got["c/d/e"].Subtree != *r.currentN["c/d/e"].Subtree, "c/d/e subtree kept")
	// Generated entries: plan mode, request owner and time, new inodes.
	for _, p := range []string{"a/conflict.txt", "gen"} {
		g := got[p]
		rtest.Assert(t, g.ModTime.Equal(now) && g.UID == f.owner[0] && g.GID == f.owner[1], "%s metadata %+v", p, g)
	}
	rtest.Equals(t, os.FileMode(0640), got["a/conflict.txt"].Mode)
	rtest.Equals(t, os.ModeDir|0700, got["gen"].Mode)
	f.checkInodes(got)

	// .plori-trash/old shares an inode with a/x: the head keeps both, the
	// twin is refused.
	rtest.Assert(t, got[".forge/state"] != nil && got[".plori-trash/old"] != nil, "private names missing from the head")
	rtest.Assert(t, resp.Public == nil, "twin written for a group with a private name")
	rtest.Equals(t, []string{"/a/x"}, resp.IncompleteLinkGroups)

	// Without the trash name, a/x is left with one name and the twin
	// leaves out .forge.
	var noTrash []mergeEntry
	for _, e := range entries {
		if !strings.HasPrefix(e.Path, ".plori-trash") {
			noTrash = append(noTrash, e)
		}
	}
	for i := range noTrash {
		if noTrash[i].Path == "a/x" {
			noTrash[i].LinkGroup = ""
		}
	}
	req2 := f.selected(r.current, r.sources, noTrash, content)
	resp2 := f.write(req2)
	got2 := f.flatten(resp2.Head.Snapshot)
	rtest.Assert(t, got2[".forge/state"] != nil, "head lost a private name")
	ax := got2["a/x"]
	rtest.Equals(t, uint64(1), ax.Links)
	rtest.Equals(t, uint64(0), ax.DeviceID)
	rtest.Equals(t, r.currentN["a/x"].Inode, ax.Inode)
	now2, err := time.Parse(time.RFC3339Nano, req2.Time)
	rtest.OK(t, err)
	rtest.Assert(t, ax.ChangeTime.Equal(now2), "a/x ctime %v", ax.ChangeTime)
	rtest.Assert(t, resp2.Public != nil && !resp2.Public.Empty, "twin missing: %+v", resp2)
	f.twinMatches(resp2)
	pub := f.flatten(resp2.Public.Snapshot)
	rtest.Assert(t, pub[".forge"] == nil && pub[".forge/state"] == nil, "twin holds a private name")
	checker.TestCheckRepo(t, f.repo)
}

// A plan that is exactly a source keeps that source's tree ID.
func TestServeWriteSelectedKeepsTree(t *testing.T) {
	r := newRoleFixture(t)
	f := r.swFixture
	all := pathsOf(r.currentN, func(string) bool { return true })
	resp := f.write(f.selected(r.current, r.sources, selectFrom("current", r.currentN, all...)))
	sn, err := data.LoadSnapshot(context.TODO(), f.repo, r.current)
	rtest.OK(t, err)
	rtest.Equals(t, sn.Tree.String(), resp.Head.Tree)
	// The same for incoming, written on current.
	inc := pathsOf(r.incomingN, func(string) bool { return true })
	resp = f.write(f.selected(r.current, r.sources, selectFrom("incoming", r.incomingN, inc...)))
	sn, err = data.LoadSnapshot(context.TODO(), f.repo, r.incoming)
	rtest.OK(t, err)
	rtest.Equals(t, sn.Tree.String(), resp.Head.Tree)
}

// Hard-link groups are rebuilt when the plan splits a source group or joins
// names of different roles.
func TestServeWriteSelectedLinkGroups(t *testing.T) {
	r := newRoleFixture(t)
	f := r.swFixture
	entries := selectFrom("incoming", r.incomingN, "new", "new/pair", "new/pair2", "new/pair3", "inc", "inc/same")
	entries = append(entries, selectFrom("base", r.baseN, "bonly", "bonly/f")...)
	entries = append(entries, selectFrom("current", r.currentN, "a", "a/b", "a/b/file1")...)
	for i := range entries {
		switch entries[i].Path {
		case "new/pair", "new/pair2":
			entries[i].LinkGroup = "split"
		case "new/pair3", "a/b/file1":
			entries[i].LinkGroup = ""
		case "bonly/f", "inc/same":
			entries[i].LinkGroup = "joined"
		}
	}
	req := f.selected(r.current, r.sources, entries)
	resp := f.write(req)
	got := f.flatten(resp.Head.Snapshot)
	now, err := time.Parse(time.RFC3339Nano, req.Time)
	rtest.OK(t, err)
	// New inodes are above the base's and every selected node's inode.
	maxNative := uint64(0)
	for _, n := range r.currentN {
		maxNative = max(maxNative, n.Inode)
	}
	for _, e := range entries {
		maxNative = max(maxNative, map[string]map[string]*data.Node{"base": r.baseN, "current": r.currentN, "incoming": r.incomingN}[e.Source.Role][e.Source.Path].Inode)
	}
	rtest.Equals(t, []string{"bonly/f,inc/same", "new/pair,new/pair2"}, linkGroupsOf(got))
	// The split pair shares a new inode with two links and a new ctime.
	split := got["new/pair"]
	rtest.Assert(t, split.Inode > maxNative && split.Inode == got["new/pair2"].Inode, "split inode %d", split.Inode)
	rtest.Equals(t, uint64(2), split.Links)
	rtest.Assert(t, split.ChangeTime.Equal(now), "split ctime %v", split.ChangeTime)
	rtest.Equals(t, r.incomingN["new/pair"].ModTime, split.ModTime)
	// The remnants have one link and device 0 and keep their inode.
	for _, p := range []string{"new/pair3", "a/b/file1"} {
		src := r.incomingN[p]
		if src == nil {
			src = r.currentN[p]
		}
		g := got[p]
		rtest.Assert(t, g.Links == 1 && g.DeviceID == 0 && g.Inode == src.Inode && g.ChangeTime.Equal(now), "%s remnant %+v", p, g)
	}
	// The joined names share a new inode; the first name by path (base's
	// bonly/f) gives the other fields.
	joined := got["bonly/f"]
	rtest.Assert(t, joined.Inode > maxNative && joined.Inode == got["inc/same"].Inode && joined.Inode != split.Inode, "joined inode %d", joined.Inode)
	rtest.Equals(t, uint64(2), joined.Links)
	rtest.Equals(t, r.baseN["bonly/f"].ModTime, got["inc/same"].ModTime)
	rtest.Equals(t, r.baseN["bonly/f"].ExtendedAttributes, got["inc/same"].ExtendedAttributes)
	rtest.Equals(t, r.baseN["bonly/f"].Content, got["inc/same"].Content)
	f.checkInodes(got)
	rtest.Assert(t, resp.Public != nil && resp.Public.Tree == resp.Head.Tree, "twin of a public tree differs")
	checker.TestCheckRepo(t, f.repo)
}

// A native inode that an earlier node of another identity holds is
// renumbered above every inode of the plan; hard-link groups of two sources
// with one inode number become two groups.
func TestServeWriteSelectedInodeCollision(t *testing.T) {
	repo, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{})
	f := &swFixture{t: t, repo: repo, be: be, srv: newTestServeWriteServer(t, be), owner: [2]uint32{1, 1}, clock: time.Now()}
	ctx := context.TODO()
	at := time.Unix(1700000000, 0).UTC()
	file := func(name string, inode, links uint64) *data.Node {
		return &data.Node{Name: name, Type: data.NodeTypeFile, Mode: 0644, ModTime: at, ChangeTime: at, UID: 7, GID: 7,
			Inode: inode, Links: links, DeviceID: map[bool]uint64{true: 5}[links > 1], Content: restic.IDs{}}
	}
	snapshot := func(nodes ...*data.Node) restic.ID {
		var root restic.ID
		rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) error {
			b := data.NewTreeJSONBuilder()
			for _, n := range nodes {
				rtest.OK(t, b.AddNode(n))
			}
			buf, _ := b.Finalize()
			var err error
			root, _, _, err = up.SaveBlob(ctx, restic.TreeBlob, buf, restic.ID{}, false)
			return err
		}))
		id, err := data.SaveSnapshot(ctx, repo, &data.Snapshot{Tree: &root, Paths: []string{"/scan"}, Time: at})
		rtest.OK(t, err)
		return id
	}
	a := snapshot(file("f", 7, 1), file("g1", 9, 2), file("g2", 9, 2))
	b := snapshot(file("h", 7, 1), file("k1", 9, 2), file("k2", 9, 2))
	an, bn := f.flatten(a.String()), f.flatten(b.String())
	entries := append(selectFrom("a", an, "f", "g1", "g2"), selectFrom("b", bn, "h", "k1", "k2")...)
	resp := f.write(f.selected(a, []treeSource{{Role: "a", Snapshot: a.String()}, {Role: "b", Snapshot: b.String()}}, entries))
	got := f.flatten(resp.Head.Snapshot)
	for p, ino := range map[string]uint64{"f": 7, "g1": 9, "g2": 9, "h": 10, "k1": 11, "k2": 11} {
		rtest.Equals(t, ino, got[p].Inode)
	}
	for p, n := range map[string]*data.Node{"f": an["f"], "g1": an["g1"], "h": bn["h"], "k2": bn["k2"]} {
		want := *n
		want.Inode = got[p].Inode
		sameNative(t, p, got[p], &want)
	}
	rtest.Equals(t, []string{"g1,g2", "k1,k2"}, linkGroupsOf(got))
	f.checkInodes(got)
}

// Paths and targets that are not valid UTF-8 travel as raw bytes.
func TestServeWriteSelectedRawNames(t *testing.T) {
	f := newSWFixture(t)
	name, target := "raw\xff", "t\xfe"
	swWrite(t, filepath.Join(f.dir, "r", name), "raw", 0644)
	rtest.OK(t, os.Symlink(target, filepath.Join(f.dir, "r", "link\xfe")))
	rtest.OK(t, os.Chmod(filepath.Join(f.dir, "r"), 0755))
	current := f.backup(nil, false)
	nodes := f.flatten(current.String())
	entries := selectFrom("current", nodes, "r", "r/"+name, "r/link\xfe")
	for i := range entries[1:] {
		e := &entries[i+1]
		e.PathRaw, e.Path = []byte(e.Path), ""
		e.Source.PathRaw, e.Source.Path = []byte(e.Source.Path), ""
	}
	entries[2].TargetRaw, entries[2].Target = []byte(target), ""
	resp := f.write(f.selected(current, []treeSource{{Role: "current", Snapshot: current.String()}}, entries))
	got := f.flatten(resp.Head.Snapshot)
	sameNative(t, "r/"+name, got["r/"+name], nodes["r/"+name])
	sameNative(t, "r/link\xfe", got["r/link\xfe"], nodes["r/link\xfe"])
	rtest.Equals(t, nodes["r"].Subtree, got["r"].Subtree)

	// A generated symlink with a raw target is encoded with linktarget_raw.
	gen := []mergeEntry{{Path: "s", Kind: "symlink", Mode: 0777, Size: int64(len(target)), TargetRaw: []byte(target), Source: &mergeSelector{Role: roleGenerated}}}
	resp = f.write(f.selected(current, []treeSource{{Role: "current", Snapshot: current.String()}}, gen))
	rtest.Equals(t, target, f.flatten(resp.Head.Snapshot)["s"].LinkTarget)
}

// Plans whose selectors do not name a matching native node, or that mix the
// plan forms, are refused before anything is written.
func TestServeWriteSelectedRefusals(t *testing.T) {
	r := newRoleFixture(t)
	f := r.swFixture
	good := func() []mergeEntry {
		return append(selectFrom("current", r.currentN, "a", "a/b", "a/b/file1", "a/b/link1"), selectFrom("incoming", r.incomingN, "inc", "inc/x")...)
	}
	f.write(f.selected(r.current, r.sources, good()))
	snapshots := f.countFiles(restic.SnapshotFile)
	at := func(entries []mergeEntry, p string) *mergeEntry {
		for i := range entries {
			if entries[i].Path == p {
				return &entries[i]
			}
		}
		panic(p)
	}
	cases := map[string]func([]mergeEntry) ([]mergeEntry, []treeSource){
		"missing source path": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "inc/x").Source.Path = "inc/nothing"
			return e, r.sources
		},
		"kind": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "inc/x").Source.Path = "inc"
			return e, r.sources
		},
		"content": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			x := at(e, "inc/x")
			x.Digest = contentToken(r.currentN["a/b/file2"].Content)
			return e, r.sources
		},
		"size": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "inc/x").Size++
			return e, r.sources
		},
		"plain digest": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			sum := sha256.Sum256([]byte("incoming x"))
			at(e, "inc/x").Digest = hex.EncodeToString(sum[:])
			return e, r.sources
		},
		"mode": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "inc/x").Mode = 0600
			return e, r.sources
		},
		"wrong role": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "inc/x").Source.Role = "base"
			return e, r.sources
		},
		"unknown role": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "inc/x").Source.Role = "other"
			return e, r.sources
		},
		"empty role": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			return e, []treeSource{{Role: "current", Snapshot: r.current.String()}, {Role: "incoming", Empty: true}}
		},
		"link group label": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "a/b/file1").Source.LinkGroup = "other"
			return e, r.sources
		},
		"missing link group label": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "a/b/file1").Source.LinkGroup = ""
			return e, r.sources
		},
		"mixed plan": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "inc/x").Source = nil
			return e, r.sources
		},
		"selector without roles": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			return e, []treeSource{{Snapshot: r.current.String()}}
		},
		"duplicate role": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			return e, append(append([]treeSource{}, r.sources...), treeSource{Role: "base", Empty: true})
		},
		"role name": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			return e, append(append([]treeSource{}, r.sources...), treeSource{Role: "Spill", Empty: true})
		},
		"generated role name": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			return e, append(append([]treeSource{}, r.sources...), treeSource{Role: roleGenerated, Empty: true})
		},
		"generated with path": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "inc/x").Source = &mergeSelector{Role: roleGenerated, Path: "inc/x"}
			return e, r.sources
		},
		"valid utf-8 raw path": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			s := at(e, "inc/x").Source
			s.PathRaw, s.Path = []byte(s.Path), ""
			return e, r.sources
		},
		"symlink component": func(e []mergeEntry) ([]mergeEntry, []treeSource) {
			at(e, "a/b/file1").Source.Path = "a/sym/x"
			return e, r.sources
		},
	}
	for name, mutate := range cases {
		entries, sources := mutate(good())
		resp := f.post("/tree-write", f.selected(r.current, sources, entries))
		rtest.Assert(t, resp.code == http.StatusBadRequest && resp.failure.Code == "invalid_request", "%s: %d %s", name, resp.code, resp.body)
	}
	// A base with a role is refused.
	req := f.selected(r.current, r.sources, good())
	req.Base.Role = "current"
	resp := f.post("/tree-write", req)
	rtest.Assert(t, resp.code == http.StatusBadRequest, "base role: %d %s", resp.code, resp.body)
	// A plan without selectors still refuses private names.
	plain := f.request(r.current)
	plain.Edits = nil
	plain.Merge = &mergePlan{Entries: entriesOf(map[string]*data.Node{".forge": r.currentN[".forge"]})}
	resp = f.post("/tree-write", plain)
	rtest.Assert(t, resp.code == http.StatusBadRequest && strings.Contains(resp.body, "private path"), "matched private: %d %s", resp.code, resp.body)
	rtest.Equals(t, snapshots, f.countFiles(restic.SnapshotFile))
	rtest.Equals(t, 0, f.countFiles(restic.LockFile))

	w := serveReadRequest(f.srv, http.MethodGet, "/version", "")
	var v versionResponse
	rtest.OK(t, json.Unmarshal(w.Body.Bytes(), &v))
	rtest.Equals(t, []string{featureMergeSelectors}, v.Features)
	rtest.Equals(t, 1, v.Version)
}
