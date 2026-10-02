package main

import (
	"context"
	"errors"
	"os"
	"path"
	"sort"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

// selectRole is one merge source of a selected plan, read through a
// copy-on-write view that is never committed.
type selectRole struct {
	name string
	root restic.ID
	tree *editTree
	// labels maps each hard-linked inode of the source to its link-group
	// label over all of its names; computed on first use.
	labels  map[inodeKey]string
	guarded map[string]bool
}

// selectNode is one entry of a selected plan and the node written for it.
type selectNode struct {
	e      *mergeEntry
	role   *selectRole // nil for a generated entry
	native *data.Node  // the selected source node, never modified
	n      *data.Node
	// same reports that n is the native node byte for byte (a directory's
	// subtree aside).
	same bool
	// fresh nodes get a new inode; names with one ident share an inode.
	fresh bool
	ident string
}

// selectWrite builds the tree of a selected plan. Every native entry copies
// the node its selector names, with every field; generated entries are built
// from the plan. Link groups, inode collisions and directory times follow the
// rules of doc/plori-tree-write.md ("Selected merge plans").
func (s *serveWriteHandler) selectWrite(ctx context.Context, req *treeWriteRequest, contents []contentItem, baseRoot restic.ID, marks map[string]float64) (restic.ID, error) {
	start := time.Now()
	st, err := s.rootStats(ctx, baseRoot)
	if err != nil {
		return restic.ID{}, err
	}
	now, owner := req.now, *req.Owner
	roles := map[string]*selectRole{}
	var sourceRoots []restic.ID
	for _, src := range req.Merge.Sources {
		_, root, err := s.base(ctx, src)
		if err != nil {
			return restic.ID{}, err
		}
		var none uint64
		r := &selectRole{name: src.Role, root: root, tree: s.newEditTree(root, now, owner, &none), guarded: map[string]bool{}}
		// Directories are decoded without the round-trip guard; guard runs
		// on each source directory whose nodes are encoded again.
		r.tree.noGuard = true
		roles[src.Role] = r
		if !root.IsNull() {
			sourceRoots = append(sourceRoots, root)
		}
	}
	entries := append([]mergeEntry(nil), req.Merge.Entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	for _, src := range req.Merge.Sources {
		var paths []string
		for i := range entries {
			if entries[i].Source.Role == src.Role {
				paths = append(paths, entries[i].Source.Path)
			}
		}
		if err = roles[src.Role].tree.prefetch(ctx, paths); err != nil {
			return restic.ID{}, err
		}
	}
	marks["merge_prefetch"] = since(start)

	b := &mergeBuild{s: s, req: req, contents: contents, sources: sourceRoots}
	nodes := make([]*selectNode, len(entries))
	byPath := make(map[string]*selectNode, len(entries))
	maxInode := st.maxInode
	for i := range entries {
		e := &entries[i]
		sn := &selectNode{e: e, ident: "e:" + e.Path}
		if e.LinkGroup != "" {
			sn.ident = "g:" + e.LinkGroup
		}
		if e.Source.Role == roleGenerated {
			var parent *data.Node
			if p := byPath[dirOf(e.Path)]; p != nil {
				parent = p.n
			}
			if sn.n, err = b.generated(ctx, e, parent); err != nil {
				return restic.ID{}, err
			}
			sn.fresh = true
		} else {
			sn.role = roles[e.Source.Role]
			if sn.native, err = sn.role.selectNative(ctx, s, e); err != nil {
				return restic.ID{}, err
			}
			n := *sn.native
			sn.n, sn.same = &n, true
			maxInode = max(maxInode, n.Inode)
		}
		sn.n.Name = path.Base(e.Path)
		nodes[i], byPath[e.Path] = sn, sn
	}
	marks["merge_lookup"] = since(start)

	// Link groups: a complete native group keeps its inode; any other group
	// shares one new node built from its first name.
	var labels []string
	groups := map[string][]*selectNode{}
	for _, sn := range nodes {
		if l := sn.e.LinkGroup; l != "" {
			if groups[l] == nil {
				labels = append(labels, l)
			}
			groups[l] = append(groups[l], sn)
		}
	}
	for _, l := range labels {
		g := groups[l]
		if completeNativeGroup(g) {
			continue
		}
		tmpl := *g[0].n
		if g[0].native != nil {
			tmpl.ChangeTime = ctimeAt(now, tmpl.ChangeTime)
		}
		tmpl.Links = uint64(len(g))
		if tmpl.DeviceID == 0 {
			tmpl.DeviceID = st.device
		}
		for _, sn := range g {
			n := tmpl
			n.Name = sn.n.Name
			sn.n, sn.same, sn.fresh = &n, false, true
		}
	}
	// A native file left with one name: one link, device ID 0 (as backup
	// stores single-link files) and a new ctime.
	for _, sn := range nodes {
		if sn.native != nil && sn.e.LinkGroup == "" && sn.n.Type == data.NodeTypeFile && sn.n.Links > 1 {
			sn.n.Links, sn.n.DeviceID, sn.n.ChangeTime = 1, 0, ctimeAt(now, sn.n.ChangeTime)
			sn.same = false
		}
	}
	// Inodes, in path order: a native inode that an earlier node of another
	// identity holds gets a new number, as do fresh nodes. New numbers are
	// above every inode of the base and of the selected nodes.
	ino := maxInode
	claimed := map[uint64]string{}
	assigned := map[string]uint64{}
	renumbered := map[string]bool{}
	for _, sn := range nodes {
		if v, ok := assigned[sn.ident]; ok {
			sn.n.Inode = v
			sn.same = sn.same && !renumbered[sn.ident]
			continue
		}
		inode := sn.n.Inode
		if _, taken := claimed[inode]; sn.fresh || taken {
			if !sn.fresh {
				renumbered[sn.ident], sn.same = true, false
			}
			ino++
			inode = ino
		}
		claimed[inode], assigned[sn.ident], sn.n.Inode = sn.ident, inode, inode
	}
	marks["merge_nodes"] = since(start)

	children := map[string][]*selectNode{}
	for _, sn := range nodes {
		children[dirOf(sn.e.Path)] = append(children[dirOf(sn.e.Path)], sn)
		if sn.e.Kind == "dir" && children[sn.e.Path] == nil {
			children[sn.e.Path] = []*selectNode{}
		}
	}
	dirs := make([]string, 0, len(children))
	for p := range children {
		dirs = append(dirs, p)
	}
	sort.Slice(dirs, func(i, j int) bool {
		if di, dj := treeDepth(dirs[i]), treeDepth(dirs[j]); di != dj {
			return di > dj
		}
		return dirs[i] < dirs[j]
	})
	var root restic.ID
	for _, p := range dirs {
		kids := children[p]
		id, err := s.selectDir(ctx, now, p, byPath[p], kids)
		if err != nil {
			return restic.ID{}, err
		}
		if p == "" {
			root = id
		} else {
			byPath[p].n.Subtree = &id
		}
	}
	marks["merge_encode"] = since(start)
	return root, nil
}

// selectNative resolves the selector of e and refuses a node that differs from
// the entry in type, mode, size, content, target or link group.
func (r *selectRole) selectNative(ctx context.Context, s *serveWriteHandler, e *mergeEntry) (*data.Node, error) {
	sel := e.Source
	n, err := r.tree.node(ctx, sel.Path)
	var refusal *writeRefusal
	if errors.As(err, &refusal) {
		n, err = nil, nil
	}
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, invalidf("entry %q: source %s has no node at %q", e.Path, r.name, sel.Path)
	}
	bits := uint32(modeBits(n.Mode) & os.ModePerm)
	if n.Mode&os.ModeSetuid != 0 {
		bits |= 04000
	}
	if n.Mode&os.ModeSetgid != 0 {
		bits |= 02000
	}
	if n.Mode&os.ModeSticky != 0 {
		bits |= 01000
	}
	ok := string(n.Type) == e.Kind && (e.Mode == bits || e.Mode == bits&0777)
	switch n.Type {
	case data.NodeTypeFile:
		ok = ok && n.Size == uint64(e.Size) && contentToken(n.Content) == e.Digest
	case data.NodeTypeSymlink:
		ok = ok && symlinkTarget(n) == e.Target
	}
	if !ok {
		return nil, invalidf("entry %q: the node at %s:%q differs from the entry", e.Path, r.name, sel.Path)
	}
	label := ""
	if n.Type == data.NodeTypeFile && n.Links > 1 {
		if r.labels == nil {
			st, err := s.rootStats(ctx, r.root)
			if err != nil {
				return nil, err
			}
			r.labels = map[inodeKey]string{}
			for key, names := range st.linkGroups() {
				rels := make([]string, 0, len(names))
				for _, l := range names {
					rels = append(rels, l.rel)
				}
				r.labels[key] = groupToken(rels)
			}
		}
		label = r.labels[inodeKey{n.DeviceID, n.Inode}]
	}
	if label != sel.LinkGroup {
		return nil, invalidf("entry %q: link group of %s:%q", e.Path, r.name, sel.Path)
	}
	return n, nil
}

// completeNativeGroup reports an output link group whose names select every
// name of one hard-linked inode of one source, each once.
func completeNativeGroup(g []*selectNode) bool {
	first := g[0]
	if first.native == nil || first.e.Source.LinkGroup == "" || uint64(len(g)) != first.native.Links {
		return false
	}
	seen := map[string]bool{}
	for _, sn := range g {
		if sn.role != first.role || sn.e.Source.LinkGroup != first.e.Source.LinkGroup || seen[sn.e.Source.Path] {
			return false
		}
		seen[sn.e.Source.Path] = true
	}
	return true
}

// generated builds the node of a generated entry from the plan: a new inode
// (assigned later), the request time and owner, and the entry's mode and
// content.
func (b *mergeBuild) generated(ctx context.Context, e *mergeEntry, parent *data.Node) (*data.Node, error) {
	now, owner := b.req.now, *b.req.Owner
	user, group := names(owner[0], owner[1], parent)
	n := &data.Node{Mode: unixMode(e.Mode), ModTime: now, AccessTime: now, ChangeTime: now,
		UID: owner[0], GID: owner[1], User: user, Group: group}
	switch e.Kind {
	case "dir":
		n.Type, n.Mode = data.NodeTypeDir, os.ModeDir|n.Mode
	case "symlink":
		// Raw target bytes stay in LinkTarget; the encoder writes
		// linktarget_raw itself.
		n.Type, n.Mode, n.Links, n.LinkTarget = data.NodeTypeSymlink, os.ModeSymlink|0777, 1, e.Target
	default:
		content, err := b.content(ctx, e, nil)
		if err != nil {
			return nil, err
		}
		n.Type, n.Links, n.Size, n.Content = data.NodeTypeFile, 1, uint64(e.Size), content
	}
	return n, nil
}

// selectDir encodes directory p of a selected plan, or keeps the subtree ID of
// its selected source directory when every name is an unchanged copy of that
// directory's node with the same name. The root, which has no entry, compares
// with the root of the role of its first name.
func (s *serveWriteHandler) selectDir(ctx context.Context, now time.Time, p string, self *selectNode, kids []*selectNode) (restic.ID, error) {
	var role *selectRole
	var srcPath string
	switch {
	case p == "" && len(kids) > 0 && kids[0].role != nil:
		role = kids[0].role
	case self != nil && self.native != nil:
		role, srcPath = self.role, self.e.Source.Path
	}
	var old *editDir
	var oldID restic.ID
	if role != nil {
		var err error
		if old, oldID, err = role.dir(ctx, srcPath); err != nil {
			return restic.ID{}, err
		}
	}
	if old != nil && unchangedCopy(old, role, srcPath, kids) {
		return oldID, nil
	}
	nodes := make([]*data.Node, len(kids))
	for i, k := range kids {
		if k.native != nil {
			if err := k.role.guard(ctx, s, dirOf(k.e.Source.Path)); err != nil {
				return restic.ID{}, err
			}
		}
		nodes[i] = k.n
	}
	if p != "" && self.native != nil && old != nil && !sameNames(old.nodes, nodes) {
		// Names were created or removed relative to the selected source
		// directory: its mtime and ctime change.
		self.n.ModTime, self.n.AccessTime, self.n.ChangeTime = now, now, ctimeAt(now, self.n.ChangeTime)
		self.same = false
	}
	return s.encode(ctx, nodes)
}

// unchangedCopy reports whether kids are exactly the nodes of old, the
// directory at srcPath of role, each copied unchanged from its own name.
func unchangedCopy(old *editDir, role *selectRole, srcPath string, kids []*selectNode) bool {
	if len(old.nodes) != len(kids) {
		return false
	}
	for _, k := range kids {
		want := k.n.Name
		if srcPath != "" {
			want = srcPath + "/" + want
		}
		if !k.same || k.role != role || k.e.Source.Path != want {
			return false
		}
		if k.n.Type == data.NodeTypeDir && (k.n.Subtree == nil || k.native.Subtree == nil || *k.n.Subtree != *k.native.Subtree) {
			return false
		}
	}
	return true
}

// dir returns the source directory at p and its tree ID, nil when the source
// has no directory there.
func (r *selectRole) dir(ctx context.Context, p string) (*editDir, restic.ID, error) {
	id := r.root
	if p != "" {
		n, err := r.tree.node(ctx, p)
		if err != nil || n == nil || n.Type != data.NodeTypeDir || n.Subtree == nil {
			return nil, restic.ID{}, err
		}
		id = *n.Subtree
	}
	if id.IsNull() {
		return nil, restic.ID{}, nil
	}
	d, err := r.tree.tryDir(ctx, p)
	return d, id, err
}

// guard runs the round-trip check once on a source directory whose nodes are
// encoded again.
func (r *selectRole) guard(ctx context.Context, s *serveWriteHandler, p string) error {
	if r.guarded[p] {
		return nil
	}
	d, id, err := r.dir(ctx, p)
	if err != nil {
		return err
	}
	if d == nil {
		return errors.New("selected source directory disappeared")
	}
	if err = s.guardTree(id, d.nodes); err != nil {
		return err
	}
	r.guarded[p] = true
	return nil
}

// ctimeAt is the ctime of a node changed at now: now, or just after a later
// stored ctime so that the change is visible.
func ctimeAt(now, old time.Time) time.Time {
	if now.After(old) {
		return now
	}
	return old.Add(time.Nanosecond)
}
