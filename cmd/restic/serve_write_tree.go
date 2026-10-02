package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

// writeRefusal is a precondition that did not hold. Nothing of the request is
// saved: the snapshots are written only after every edit applied.
type writeRefusal struct {
	Code        string `json:"code"`
	Edit        int    `json:"edit"`
	CurrentETag uint64 `json:"current_etag,omitempty"`
}

func (r *writeRefusal) Error() string { return "refused: " + r.Code }

const (
	codeStale    = "file_stale"
	codeNotFound = "file_not_found"
	codeExists   = "file_exists"
	codeNotDir   = "file_not_dir"
	codeRefused  = "content_refused"
	trashDir     = ".plori-trash"
	maxTreeDepth = 128
)

func refuse(code string) error { return &writeRefusal{Code: code} }

// errInvalid is a malformed request (HTTP 400).
var errInvalid = errors.New("invalid write request")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errInvalid, fmt.Sprintf(format, args...))
}

type inodeKey struct{ device, inode uint64 }

// linkName is one name of a hard-linked file below a tree. Private reports
// whether a segment of rel matches a public exclude.
type linkName struct {
	key     inodeKey
	links   uint64
	rel     string
	private bool
}

// treeStats aggregates a tree and everything below it. A tree ID names
// immutable content, so the memo stays valid for every snapshot that
// references the tree, including snapshots this process writes later.
type treeStats struct {
	entries, files, bytes          uint64
	pubEntries, pubFiles, pubBytes uint64
	maxInode                       uint64
	device                         uint64
	linked                         []linkName
}

func (s *serveWriteHandler) excluded(name string) bool {
	lower := strings.ToLower(name)
	for _, p := range s.excludes {
		if ok, _ := path.Match(strings.ToLower(p), lower); ok {
			return true
		}
	}
	return false
}

// loadNodes decodes a tree. With guard it re-encodes the tree and refuses it
// unless the bytes hash to the same ID (walker.TreeRewriter's round-trip
// check): an unknown node field would otherwise be dropped on rewrite.
func (s *serveWriteHandler) loadNodes(ctx context.Context, id restic.ID, guard bool) ([]*data.Node, error) {
	tree, err := data.LoadTree(ctx, s, id)
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
	if guard {
		if err = guardNodes(id, nodes); err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

// guardNodes re-encodes a decoded tree and refuses it unless the bytes hash to
// its ID.
func guardNodes(id restic.ID, nodes []*data.Node) error {
	b := data.NewTreeJSONBuilder()
	for _, n := range nodes {
		if err := b.AddNode(n); err != nil {
			return err
		}
	}
	buf, _ := b.Finalize()
	if restic.Hash(buf) != id {
		return fmt.Errorf("cannot encode tree %v without losing information", id.Str())
	}
	return nil
}

// guardTree checks a directory loaded without the guard.
func (s *serveWriteHandler) guardTree(id restic.ID, byName map[string]*data.Node) error {
	nodes := make([]*data.Node, 0, len(byName))
	for _, n := range byName {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return guardNodes(id, nodes)
}

func (s *serveWriteHandler) cachedStats(id restic.ID) (*treeStats, bool) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	st, ok := s.stats[id]
	return st, ok
}

func (s *serveWriteHandler) statsOf(ctx context.Context, id restic.ID) (*treeStats, error) {
	if st, ok := s.cachedStats(id); ok {
		return st, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nodes, err := s.loadNodes(ctx, id, false)
	if err != nil {
		return nil, err
	}
	return s.remember(ctx, id, nodes)
}

// remember computes and records the statistics of a tree. Subtrees missing
// from the memo are walked in parallel while a worker slot is free, else on
// the calling goroutine, so a first walk of a large tree uses several cores.
func (s *serveWriteHandler) remember(ctx context.Context, id restic.ID, nodes []*data.Node) (*treeStats, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var walkErr error
	for _, n := range nodes {
		if n.Type != data.NodeTypeDir || n.Subtree == nil {
			continue
		}
		if _, ok := s.cachedStats(*n.Subtree); ok {
			continue
		}
		select {
		case s.walkers <- struct{}{}:
			wg.Add(1)
			go func(sub restic.ID) {
				defer wg.Done()
				defer func() { <-s.walkers }()
				if _, err := s.statsOf(ctx, sub); err != nil {
					mu.Lock()
					walkErr = err
					mu.Unlock()
				}
			}(*n.Subtree)
		default:
		}
	}
	wg.Wait()
	if walkErr != nil {
		return nil, walkErr
	}
	st := &treeStats{}
	for _, n := range nodes {
		private := s.excluded(n.Name)
		st.entries++
		if !private {
			st.pubEntries++
		}
		st.maxInode = max(st.maxInode, n.Inode)
		switch n.Type {
		case data.NodeTypeFile:
			st.files++
			st.bytes += n.Size
			if !private {
				st.pubFiles++
				st.pubBytes += n.Size
			}
			if n.Links > 1 {
				st.linked = append(st.linked, linkName{inodeKey{n.DeviceID, n.Inode}, n.Links, n.Name, private})
				st.device = n.DeviceID
			}
		case data.NodeTypeDir:
			if n.Subtree == nil {
				return nil, fmt.Errorf("directory %q has no subtree", n.Name)
			}
			child, err := s.statsOf(ctx, *n.Subtree)
			if err != nil {
				return nil, err
			}
			st.entries += child.entries
			st.files += child.files
			st.bytes += child.bytes
			if !private {
				st.pubEntries += child.pubEntries
				st.pubFiles += child.pubFiles
				st.pubBytes += child.pubBytes
			}
			st.maxInode = max(st.maxInode, child.maxInode)
			if child.device != 0 {
				st.device = child.device
			}
			for _, l := range child.linked {
				st.linked = append(st.linked, linkName{l.key, l.links, n.Name + "/" + l.rel, l.private || private})
			}
		}
	}
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	if len(s.stats) >= serveWriteStatsLimit {
		clear(s.stats)
	}
	s.stats[id] = st
	return st, nil
}

// linkGroups returns every name of each hard-linked inode below the tree.
func (st *treeStats) linkGroups() map[inodeKey][]linkName {
	groups := map[inodeKey][]linkName{}
	for _, l := range st.linked {
		groups[l.key] = append(groups[l.key], l)
	}
	return groups
}

// groupToken is the merge manifest's link-group label: SHA-256 of the JSON
// array of the sorted names (resticrev.groupToken in plori-runtime).
func groupToken(names []string) string {
	names = append([]string(nil), names...)
	sort.Strings(names)
	b, _ := json.Marshal(names)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// contentToken is the merge manifest digest of a restic file: "restic:" and
// SHA-256 over the ordered blob IDs.
func contentToken(ids restic.IDs) string {
	h := sha256.New()
	for _, id := range ids {
		_, _ = h.Write(id[:])
	}
	return "restic:" + hex.EncodeToString(h.Sum(nil))
}

// etagOf is the Files ETag of a stored node (workspacehelper.etagOf).
func etagOf(n *data.Node) uint64 {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%d-%d-%d.%d-%d.%d", n.Inode, n.Size, n.ModTime.Unix(), n.ModTime.Nanosecond(), n.ChangeTime.Unix(), n.ChangeTime.Nanosecond())
	if v := h.Sum64(); v != 0 {
		return v
	}
	return 1
}

// unixMode converts Unix permission bits (07777) to the os.FileMode bits
// restic stores.
func unixMode(m uint32) os.FileMode {
	v := os.FileMode(m & 0777)
	if m&04000 != 0 {
		v |= os.ModeSetuid
	}
	if m&02000 != 0 {
		v |= os.ModeSetgid
	}
	if m&01000 != 0 {
		v |= os.ModeSticky
	}
	return v
}

func modeBits(m os.FileMode) os.FileMode {
	return m & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
}

func treeDepth(p string) int {
	if p == "" {
		return 0
	}
	return strings.Count(p, "/") + 1
}

func dirOf(p string) string {
	if d := path.Dir(p); d != "." {
		return d
	}
	return ""
}

// editTree is a copy-on-write view of one tree. Directories are loaded only
// along the paths an edit touches; commit writes the changed directories and
// their ancestors, every other subtree keeps its ID.
type editTree struct {
	s     *serveWriteHandler
	root  restic.ID // null for the empty tree
	dirs  map[string]*editDir
	now   time.Time
	owner [2]uint32
	ino   *uint64
	// noGuard decodes directories without the round-trip check; the caller
	// must check a directory before encoding its nodes again.
	noGuard bool
}

type editDir struct {
	nodes   map[string]*data.Node
	changed bool
}

func (s *serveWriteHandler) newEditTree(root restic.ID, now time.Time, owner [2]uint32, ino *uint64) *editTree {
	return &editTree{s: s, root: root, dirs: map[string]*editDir{}, now: now, owner: owner, ino: ino}
}

func (t *editTree) rootStats(ctx context.Context) (*treeStats, error) {
	if t.root.IsNull() {
		return &treeStats{}, nil
	}
	return t.s.statsOf(ctx, t.root)
}

// tryDir returns the directory at p, nil when a component is missing. A
// symlink component refuses the path, as the gateway resolved Files paths
// with RESOLVE_NO_SYMLINKS; a file component is not a directory.
func (t *editTree) tryDir(ctx context.Context, p string) (*editDir, error) {
	if d, ok := t.dirs[p]; ok {
		return d, nil
	}
	var id restic.ID
	if p == "" {
		id = t.root
	} else {
		parent, err := t.tryDir(ctx, dirOf(p))
		if parent == nil || err != nil {
			return nil, err
		}
		n := parent.nodes[path.Base(p)]
		switch {
		case n == nil:
			return nil, nil
		case n.Type == data.NodeTypeSymlink:
			return nil, refuse(codeRefused)
		case n.Type != data.NodeTypeDir:
			return nil, refuse(codeNotDir)
		case n.Subtree == nil:
			return nil, fmt.Errorf("directory %q has no subtree", p)
		}
		id = *n.Subtree
	}
	d := &editDir{nodes: map[string]*data.Node{}}
	if !id.IsNull() {
		nodes, err := t.s.loadNodes(ctx, id, !t.noGuard)
		if err != nil {
			return nil, err
		}
		for _, n := range nodes {
			d.nodes[n.Name] = n
		}
	}
	t.dirs[p] = d
	return d, nil
}

// prefetch loads the directories of the given paths, one depth level at a
// time with up to eight decoders, so a lookup of many paths does not decode
// the trees one after another. Components that are not directories are left
// to tryDir.
func (t *editTree) prefetch(ctx context.Context, paths []string) error {
	if _, err := t.tryDir(ctx, ""); err != nil {
		return err
	}
	levels := map[int]map[string]bool{}
	for _, p := range paths {
		for d := dirOf(p); d != ""; d = dirOf(d) {
			k := treeDepth(d)
			if levels[k] == nil {
				levels[k] = map[string]bool{}
			}
			if levels[k][d] {
				break
			}
			levels[k][d] = true
		}
	}
	for depth := 1; levels[depth] != nil; depth++ {
		var dirs []string
		var ids []restic.ID
		for d := range levels[depth] {
			parent := t.dirs[dirOf(d)]
			if _, done := t.dirs[d]; done || parent == nil {
				continue
			}
			if n := parent.nodes[path.Base(d)]; n != nil && n.Type == data.NodeTypeDir && n.Subtree != nil {
				dirs, ids = append(dirs, d), append(ids, *n.Subtree)
			}
		}
		loaded := make([][]*data.Node, len(ids))
		var g errgroup.Group
		g.SetLimit(8)
		for i := range ids {
			g.Go(func() error {
				var err error
				loaded[i], err = t.s.loadNodes(ctx, ids[i], !t.noGuard)
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		for i, d := range dirs {
			ed := &editDir{nodes: make(map[string]*data.Node, len(loaded[i]))}
			for _, n := range loaded[i] {
				ed.nodes[n.Name] = n
			}
			t.dirs[d] = ed
		}
	}
	return nil
}

func (t *editTree) node(ctx context.Context, p string) (*data.Node, error) {
	d, err := t.tryDir(ctx, dirOf(p))
	if d == nil || err != nil {
		return nil, err
	}
	return d.nodes[path.Base(p)], nil
}

func (t *editTree) put(p string, n *data.Node) {
	d := t.dirs[dirOf(p)]
	n.Name = path.Base(p)
	d.nodes[n.Name] = n
	d.changed = true
}

func (t *editTree) ctime(old time.Time) time.Time {
	if t.now.After(old) {
		return t.now
	}
	return old.Add(time.Nanosecond)
}

func (t *editTree) newInode() uint64 {
	*t.ino++
	return *t.ino
}

// touch records a change of directory p's entries, as the kernel updates a
// directory's mtime and ctime on create, unlink and rename.
func (t *editTree) touch(p string) {
	if p == "" {
		return // the root tree has no node
	}
	n := t.dirs[dirOf(p)].nodes[path.Base(p)]
	n.ModTime, n.AccessTime, n.ChangeTime = t.now, t.now, t.ctime(n.ChangeTime)
	t.dirs[dirOf(p)].changed = true
}

// names returns the user and group names of the first candidate with the same
// IDs; restic backup records the names its host resolves for those IDs.
func names(uid, gid uint32, candidates ...*data.Node) (user, group string) {
	for _, c := range candidates {
		if c == nil {
			continue
		}
		if user == "" && c.UID == uid {
			user = c.User
		}
		if group == "" && c.GID == gid {
			group = c.Group
		}
	}
	return user, group
}

func (t *editTree) dirNode(p string, mode os.FileMode, uid, gid uint32) *data.Node {
	var parent *data.Node
	if q := dirOf(p); q != "" {
		parent = t.dirs[dirOf(q)].nodes[path.Base(q)]
	}
	user, group := names(uid, gid, parent)
	return &data.Node{Name: path.Base(p), Type: data.NodeTypeDir, Mode: os.ModeDir | mode,
		ModTime: t.now, AccessTime: t.now, ChangeTime: t.now, UID: uid, GID: gid, User: user, Group: group, Inode: t.newInode()}
}

// mkdir creates directory p; its parent must be loaded.
func (t *editTree) mkdir(p string, mode os.FileMode, uid, gid uint32) {
	t.put(p, t.dirNode(p, mode, uid, gid))
	t.dirs[p] = &editDir{nodes: map[string]*data.Node{}, changed: true}
	t.touch(dirOf(p))
}

// ensureParents creates the missing parents of p, owned by the worker identity
// with mode 0755 (workspacehelper.ensureParents).
func (t *editTree) ensureParents(ctx context.Context, p string) error {
	parent := dirOf(p)
	if parent == "" {
		return nil
	}
	d, err := t.tryDir(ctx, parent)
	if err != nil || d != nil {
		return err
	}
	if err = t.ensureParents(ctx, parent); err != nil {
		return err
	}
	t.mkdir(parent, 0755, t.owner[0], t.owner[1])
	return nil
}

// aliases returns the paths of every name of n's inode, including private and
// trash names, and checks that the tree holds all of them.
func (t *editTree) aliases(ctx context.Context, n *data.Node) ([]string, error) {
	st, err := t.rootStats(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, l := range st.linked {
		if l.key == (inodeKey{n.DeviceID, n.Inode}) {
			out = append(out, l.rel)
		}
	}
	if uint64(len(out)) != n.Links {
		return nil, fmt.Errorf("hard-link group of inode %d has %d names, %d links", n.Inode, len(out), n.Links)
	}
	return out, nil
}

// inodeUpdate applies fn to n and, for a hard-linked file, to every other name
// of the inode, so that all names keep identical inode metadata.
func (t *editTree) inodeUpdate(ctx context.Context, p string, n *data.Node, fn func(*data.Node)) error {
	if n.Type != data.NodeTypeFile || n.Links <= 1 {
		fn(n)
		t.dirs[dirOf(p)].changed = true
		return nil
	}
	paths, err := t.aliases(ctx, n)
	if err != nil {
		return err
	}
	for _, p := range paths {
		a, err := t.node(ctx, p)
		if err != nil {
			return err
		}
		if a == nil || a.Inode != n.Inode || a.DeviceID != n.DeviceID {
			return fmt.Errorf("hard-link name %q not found", p)
		}
		fn(a)
		t.dirs[dirOf(p)].changed = true
	}
	return nil
}

func checkPath(p string) error {
	if p == "" || path.IsAbs(p) || path.Clean(p) != p || strings.ContainsRune(p, 0) || strings.Count(p, "/")+1 > maxTreeDepth {
		return invalidf("path %q", p)
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".." || strings.EqualFold(part, "lost+found") {
			return invalidf("path %q", p)
		}
	}
	return nil
}

func precondition(e *writeEdit, n *data.Node) error {
	if e.ExpectedETag == nil {
		return nil
	}
	if n == nil {
		if *e.ExpectedETag == 0 && e.Op == "write" {
			return nil
		}
		return &writeRefusal{Code: codeStale}
	}
	if cur := etagOf(n); *e.ExpectedETag != cur {
		return &writeRefusal{Code: codeStale, CurrentETag: cur}
	}
	return nil
}

// apply makes one edit. It follows the helper's Files rules
// (workspacehelper/mutation.go): a write to a file with several names rewrites
// that inode, so every alias sees the bytes; any other write creates a new
// inode; delete moves the entry to .plori-trash/<handle>; rename keeps the
// inode; mkdir creates parents.
func (t *editTree) apply(ctx context.Context, e *writeEdit, save contentSaver) (target string, err error) {
	if err = checkPath(e.Path); err != nil {
		return "", err
	}
	n, err := t.node(ctx, e.Path)
	if err != nil {
		return "", err
	}
	switch e.Op {
	case "write":
		if n != nil && n.Type != data.NodeTypeFile {
			return "", refuse(codeRefused)
		}
		if err = precondition(e, n); err != nil {
			return "", err
		}
		content, size, err := save(ctx, e)
		if err != nil {
			return "", err
		}
		if n != nil && n.Links > 1 {
			return e.Path, t.inodeUpdate(ctx, e.Path, n, func(a *data.Node) {
				a.Content, a.Size = content, size
				a.ModTime, a.AccessTime, a.ChangeTime = t.now, t.now, t.ctime(a.ChangeTime)
				if e.Mode != 0 {
					a.Mode = a.Mode&^os.ModePerm | os.FileMode(e.Mode&0777)
				}
			})
		}
		if err = t.ensureParents(ctx, e.Path); err != nil {
			return "", err
		}
		mode := os.FileMode(e.Mode & 0777)
		if mode == 0 {
			mode = 0644
			if n != nil {
				mode = n.Mode.Perm()
			}
		}
		var parent *data.Node
		if q := dirOf(e.Path); q != "" {
			parent = t.dirs[dirOf(q)].nodes[path.Base(q)]
		}
		user, group := names(t.owner[0], t.owner[1], n, parent)
		t.put(e.Path, &data.Node{Type: data.NodeTypeFile, Mode: mode, ModTime: t.now, AccessTime: t.now, ChangeTime: t.now,
			UID: t.owner[0], GID: t.owner[1], User: user, Group: group, Inode: t.newInode(), Links: 1, Size: size, Content: content})
		t.touch(dirOf(e.Path))
		return e.Path, nil
	case "mkdir":
		if n != nil {
			if n.Type == data.NodeTypeDir {
				return e.Path, nil
			}
			return "", refuse(codeExists)
		}
		if err = t.ensureParents(ctx, e.Path); err != nil {
			return "", err
		}
		mode := os.FileMode(e.Mode & 0777)
		if mode == 0 {
			mode = 0755
		}
		t.mkdir(e.Path, mode, t.owner[0], t.owner[1])
		return e.Path, nil
	case "delete", "rename", "chmod":
		if n == nil {
			if e.ExpectedETag != nil {
				return "", refuse(codeStale)
			}
			return "", refuse(codeNotFound)
		}
		if n.Type != data.NodeTypeFile && n.Type != data.NodeTypeDir {
			return "", refuse(codeRefused)
		}
		if err = precondition(e, n); err != nil {
			return "", err
		}
	default:
		return "", invalidf("op %q", e.Op)
	}
	if e.Op == "chmod" {
		mode := unixMode(e.Mode)
		return e.Path, t.inodeUpdate(ctx, e.Path, n, func(a *data.Node) {
			a.Mode = a.Mode&^modeBits(^os.FileMode(0)) | mode
			a.ChangeTime = t.ctime(a.ChangeTime)
		})
	}
	to := e.To
	if e.Op == "delete" {
		if e.Trash == "" || strings.Contains(e.Trash, "/") || checkPath(e.Trash) != nil {
			return "", invalidf("trash handle %q", e.Trash)
		}
		trash, err := t.node(ctx, trashDir)
		if err != nil {
			return "", err
		}
		if trash == nil {
			// The helper creates .plori-trash as root with mode 0700.
			if _, err = t.tryDir(ctx, ""); err != nil {
				return "", err
			}
			t.mkdir(trashDir, 0700, 0, 0)
			t.dirs[""].nodes[trashDir].User, t.dirs[""].nodes[trashDir].Group = "root", "root"
		} else if trash.Type != data.NodeTypeDir {
			return "", refuse(codeRefused)
		}
		to = trashDir + "/" + e.Trash
	}
	if err = checkPath(to); err != nil {
		return "", err
	}
	if to == e.Path || strings.HasPrefix(to, e.Path+"/") {
		return "", invalidf("rename %q into itself", e.Path)
	}
	dst, err := t.node(ctx, to)
	if err != nil {
		return "", err
	}
	if dst != nil {
		return "", refuse(codeExists)
	}
	if err = t.ensureParents(ctx, to); err != nil {
		return "", err
	}
	// rename(2) changes the moved inode's ctime; a hard-linked file shares it.
	if err = t.inodeUpdate(ctx, e.Path, n, func(a *data.Node) { a.ChangeTime = t.ctime(a.ChangeTime) }); err != nil {
		return "", err
	}
	from := t.dirs[dirOf(e.Path)]
	delete(from.nodes, path.Base(e.Path))
	from.changed = true
	if n.Type == data.NodeTypeDir {
		moved := map[string]*editDir{}
		for p, d := range t.dirs {
			if p == e.Path || strings.HasPrefix(p, e.Path+"/") {
				moved[to+strings.TrimPrefix(p, e.Path)] = d
				delete(t.dirs, p)
			}
		}
		for p, d := range moved {
			t.dirs[p] = d
		}
	}
	t.put(to, n)
	t.touch(dirOf(e.Path))
	t.touch(dirOf(to))
	if e.Op == "delete" {
		return "", nil
	}
	return to, nil
}

// commit encodes the changed directories bottom-up into pending tree blobs and
// returns the new root (null for an empty tree).
func (t *editTree) commit(ctx context.Context) (restic.ID, error) {
	for p, d := range t.dirs {
		if !d.changed {
			continue
		}
		for q := p; q != ""; {
			q = dirOf(q)
			t.dirs[q].changed = true
		}
	}
	var changed []string
	for p, d := range t.dirs {
		if d.changed {
			changed = append(changed, p)
		}
	}
	if len(changed) == 0 {
		return t.root, nil
	}
	sort.Slice(changed, func(i, j int) bool { return treeDepth(changed[i]) > treeDepth(changed[j]) })
	var root restic.ID
	for _, p := range changed {
		d := t.dirs[p]
		nodes := make([]*data.Node, 0, len(d.nodes))
		for _, n := range d.nodes {
			nodes = append(nodes, n)
		}
		id, err := t.s.encode(ctx, nodes)
		if err != nil {
			return restic.ID{}, err
		}
		if p == "" {
			if len(nodes) > 0 {
				root = id
			}
			continue
		}
		t.dirs[dirOf(p)].nodes[path.Base(p)].Subtree = &id
	}
	return root, nil
}

// encode sorts nodes, builds the tree blob, keeps it pending until the request
// saves the blobs its snapshots reference, and records its statistics.
func (s *serveWriteHandler) encode(ctx context.Context, nodes []*data.Node) (restic.ID, error) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	b := data.NewTreeJSONBuilder()
	for _, n := range nodes {
		if err := b.AddNode(n); err != nil {
			return restic.ID{}, err
		}
	}
	buf, _ := b.Finalize()
	id := restic.Hash(buf)
	if _, ok := s.repo.LookupBlobSize(restic.TreeBlob, id); !ok {
		s.pending[id] = buf
	}
	_, err := s.remember(ctx, id, nodes)
	return id, err
}
