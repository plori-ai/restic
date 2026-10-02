package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

const mergeMaxEntries = 1000000

// validateEntries applies the helper's manifest checks (workspacehelper
// validate): canonical relative paths, no public-excluded segment, a directory
// parent for every entry, and consistent link groups of at least two names.
func (s *serveWriteHandler) validateEntries(entries []mergeEntry) error {
	if len(entries) == 0 || len(entries) > mergeMaxEntries {
		return invalidf("entry count %d", len(entries))
	}
	byPath := map[string]*mergeEntry{}
	groups := map[string][]*mergeEntry{}
	for i := range entries {
		e := &entries[i]
		if err := checkPath(e.Path); err != nil {
			return err
		}
		for _, part := range strings.Split(e.Path, "/") {
			if s.excluded(part) {
				return invalidf("private path %q", e.Path)
			}
		}
		if e.Size < 0 || e.Mode&^uint32(07777) != 0 {
			return invalidf("metadata of %q", e.Path)
		}
		switch e.Kind {
		case "file":
			if _, err := restic.ParseID(strings.TrimPrefix(e.Digest, "restic:")); err != nil || strings.ToLower(e.Digest) != e.Digest {
				return invalidf("digest of %q", e.Path)
			}
		case "dir":
			if e.Size != 0 {
				return invalidf("directory size of %q", e.Path)
			}
		case "symlink":
			if e.Target == "" || strings.ContainsRune(e.Target, 0) || e.Size != int64(len(e.Target)) {
				return invalidf("symlink %q", e.Path)
			}
		default:
			return invalidf("kind of %q", e.Path)
		}
		if e.LinkGroup != "" {
			if e.Kind != "file" {
				return invalidf("non-file link group %q", e.Path)
			}
			groups[e.LinkGroup] = append(groups[e.LinkGroup], e)
		}
		if byPath[e.Path] != nil {
			return invalidf("duplicate path %q", e.Path)
		}
		byPath[e.Path] = e
	}
	for p := range byPath {
		if parent := dirOf(p); parent != "" && (byPath[parent] == nil || byPath[parent].Kind != "dir") {
			return invalidf("missing directory parent of %q", p)
		}
	}
	for _, g := range groups {
		if len(g) < 2 {
			return invalidf("incomplete link group at %q", g[0].Path)
		}
		for _, e := range g[1:] {
			if e.Size != g[0].Size || e.Mode != g[0].Mode || e.Digest != g[0].Digest {
				return invalidf("inconsistent link group at %q", e.Path)
			}
		}
	}
	return nil
}

// mergeWrite writes a snapshot whose tree is exactly the merged entry list. A
// name whose base node matches the entry (kind, content, link group, target)
// keeps that native node with every field; mode and owner follow the entry
// and the worker identity, as the helper's setMetadata sets them. Every other
// node is synthesized with a new inode; the names of a new link group share
// one. Content comes from the base and source snapshots by the entry's blob
// token, or from an inline diff3 blob keyed by its byte SHA-256.
func (s *serveWriteHandler) mergeWrite(ctx context.Context, req *writeRequest) (*writeResponse, error) {
	start := time.Now()
	if req.OpID == "" {
		return nil, invalidf("op_id is required")
	}
	if err := s.validateEntries(req.Entries); err != nil {
		return nil, err
	}
	baseSn, baseRoot, err := s.base(ctx, req.Base)
	if err != nil {
		return nil, err
	}
	var sources []restic.ID
	for _, id := range req.Sources {
		_, root, err := s.base(ctx, id)
		if err != nil {
			return nil, err
		}
		sources = append(sources, root)
	}
	resp := &writeResponse{TimingsMS: map[string]float64{"load": since(start)}}
	b := &mergeBuild{s: s, req: req, sources: sources, children: map[string][]*data.Node{}, dirNodes: map[string]*data.Node{}, synth: map[string]*data.Node{},
		same: map[*data.Node]bool{}, groupSize: map[string]int{}, marks: resp.TimingsMS}
	root, err := b.build(ctx, baseRoot)
	if err != nil {
		return nil, err
	}
	f, err := s.check(ctx, root)
	if err != nil {
		return nil, err
	}
	resp.TimingsMS["build"] = since(start)
	if err = s.upload(ctx, f); err != nil {
		return nil, err
	}
	resp.TimingsMS["flush"] = since(start)
	return s.snapshots(ctx, req, baseSn, f, start, resp)
}

type mergeBuild struct {
	s        *serveWriteHandler
	req      *writeRequest
	base     *editTree
	sources  []restic.ID
	children map[string][]*data.Node
	dirNodes map[string]*data.Node
	synth    map[string]*data.Node
	// same marks nodes copied from the base without a change; a directory
	// whose nodes are all such copies keeps the base subtree ID.
	same      map[*data.Node]bool
	groupSize map[string]int
	marks     map[string]float64
}

// baseNode is the base snapshot's node at p; a path the base cannot resolve
// as directories has none.
func (b *mergeBuild) baseNode(ctx context.Context, p string) (*data.Node, error) {
	n, err := b.base.node(ctx, p)
	var refusal *writeRefusal
	if errors.As(err, &refusal) {
		return nil, nil
	}
	return n, err
}

func (b *mergeBuild) build(ctx context.Context, baseRoot restic.ID) (restic.ID, error) {
	s := b.s
	st, err := s.rootStats(ctx, baseRoot)
	if err != nil {
		return restic.ID{}, err
	}
	ino := st.maxInode
	owner := b.req.owner()
	b.base = s.newEditTree(baseRoot, time.Now(), owner, &ino)
	// Base directories are decoded without the round-trip guard; the guard
	// runs on each base directory whose nodes are encoded again below.
	b.base.noGuard = true
	start := time.Now()
	labels := map[inodeKey]string{}
	for key, names := range st.linkGroups() {
		rels := make([]string, 0, len(names))
		for _, l := range names {
			rels = append(rels, l.rel)
		}
		labels[key] = groupToken(rels)
	}
	entries := append([]mergeEntry(nil), b.req.Entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	for _, e := range entries {
		if e.LinkGroup != "" {
			b.groupSize[e.LinkGroup]++
		}
	}
	paths := make([]string, len(entries))
	for i := range entries {
		paths[i] = entries[i].Path
	}
	if err = b.base.prefetch(ctx, paths); err != nil {
		return restic.ID{}, err
	}
	b.marks["merge_prefetch"] = since(start)
	bases := make([]*data.Node, len(entries))
	keep := make([]bool, len(entries))
	groupKeep := map[string]bool{}
	groupKey := map[string]inodeKey{}
	for i, e := range entries {
		if bases[i], err = b.baseNode(ctx, e.Path); err != nil {
			return restic.ID{}, err
		}
		n := bases[i]
		if e.Kind != "file" || n == nil || n.Type != data.NodeTypeFile {
			continue
		}
		label := ""
		if n.Links > 1 {
			label = labels[inodeKey{n.DeviceID, n.Inode}]
		}
		keep[i] = contentToken(n.Content) == e.Digest && n.Size == uint64(e.Size) && label == e.LinkGroup
		if e.LinkGroup == "" {
			continue
		}
		key := inodeKey{n.DeviceID, n.Inode}
		prev, seen := groupKeep[e.LinkGroup]
		if !seen {
			prev, groupKey[e.LinkGroup] = true, key
		}
		groupKeep[e.LinkGroup] = prev && keep[i] && groupKey[e.LinkGroup] == key
	}
	for i, e := range entries {
		if e.LinkGroup != "" && !groupKeep[e.LinkGroup] {
			keep[i] = false
		}
	}
	b.marks["merge_lookup"] = since(start)
	t := b.base
	for i := range entries {
		e := &entries[i]
		n, err := b.node(ctx, e, bases[i], keep[i], st.device)
		if err != nil {
			return restic.ID{}, err
		}
		n.Name = path.Base(e.Path)
		b.children[dirOf(e.Path)] = append(b.children[dirOf(e.Path)], n)
		if e.Kind == "dir" {
			b.dirNodes[e.Path] = n
			if b.children[e.Path] == nil {
				b.children[e.Path] = []*data.Node{}
			}
		}
	}
	b.marks["merge_nodes"] = since(start)
	dirs := make([]string, 0, len(b.children))
	for p := range b.children {
		dirs = append(dirs, p)
	}
	sort.Slice(dirs, func(i, j int) bool { return treeDepth(dirs[i]) > treeDepth(dirs[j]) })
	var root restic.ID
	for _, p := range dirs {
		nodes := b.children[p]
		old, oldID, err := b.baseDir(ctx, p)
		if err != nil {
			return restic.ID{}, err
		}
		var id restic.ID
		if old != nil && b.unchanged(old, nodes) {
			id = oldID
		} else {
			if old != nil {
				if err = s.guardTree(oldID, old.nodes); err != nil {
					return restic.ID{}, err
				}
			}
			// A directory whose names differ from the base directory's had
			// entries created or removed: its mtime and ctime change.
			if n := b.dirNodes[p]; p != "" && old != nil && !sameNames(old.nodes, nodes) {
				n.ModTime, n.AccessTime, n.ChangeTime = t.now, t.now, t.ctime(n.ChangeTime)
				b.same[n] = false
			}
			if id, err = s.encode(ctx, nodes); err != nil {
				return restic.ID{}, err
			}
		}
		if p == "" {
			root = id
		} else {
			b.dirNodes[p].Subtree = &id
		}
	}
	b.marks["merge_encode"] = since(start)
	return root, nil
}

// baseDir returns the base directory at p and its tree ID, nil when the base
// has no directory there.
func (b *mergeBuild) baseDir(ctx context.Context, p string) (*editDir, restic.ID, error) {
	t := b.base
	if p == "" {
		if t.root.IsNull() {
			return nil, restic.ID{}, nil
		}
		d, err := t.tryDir(ctx, "")
		return d, t.root, err
	}
	n, err := b.baseNode(ctx, p)
	if err != nil || n == nil || n.Type != data.NodeTypeDir || n.Subtree == nil {
		return nil, restic.ID{}, err
	}
	d, err := t.tryDir(ctx, p)
	return d, *n.Subtree, err
}

// unchanged reports whether nodes are exactly the base directory's nodes.
func (b *mergeBuild) unchanged(old *editDir, nodes []*data.Node) bool {
	if len(old.nodes) != len(nodes) {
		return false
	}
	for _, n := range nodes {
		o := old.nodes[n.Name]
		if o == nil || !b.same[n] {
			return false
		}
		if n.Type == data.NodeTypeDir && (o.Subtree == nil || n.Subtree == nil || *o.Subtree != *n.Subtree) {
			return false
		}
	}
	return true
}

func sameNames(old map[string]*data.Node, nodes []*data.Node) bool {
	if len(old) != len(nodes) {
		return false
	}
	for _, n := range nodes {
		if old[n.Name] == nil {
			return false
		}
	}
	return true
}

// policy gives a retained node the entry's mode and the worker owner; a change
// sets ctime, as chmod/chown do.
func (b *mergeBuild) policy(n *data.Node, e *mergeEntry) bool {
	owner := b.req.owner()
	mode := unixMode(e.Mode)
	changed := n.UID != owner[0] || n.GID != owner[1]
	if n.Type != data.NodeTypeSymlink && modeBits(n.Mode) != mode {
		n.Mode = n.Mode&^modeBits(^os.FileMode(0)) | mode
		changed = true
	}
	if changed {
		if n.UID != owner[0] {
			n.UID, n.User = owner[0], ""
		}
		if n.GID != owner[1] {
			n.GID, n.Group = owner[1], ""
		}
		n.ChangeTime = b.base.ctime(n.ChangeTime)
	}
	return changed
}

func (b *mergeBuild) node(ctx context.Context, e *mergeEntry, base *data.Node, keep bool, device uint64) (*data.Node, error) {
	t := b.base
	owner := b.req.owner()
	var parent *data.Node
	if q := dirOf(e.Path); q != "" {
		parent = b.dirNodes[q]
	}
	user, group := names(owner[0], owner[1], base, parent)
	fresh := &data.Node{Mode: unixMode(e.Mode), ModTime: t.now, AccessTime: t.now, ChangeTime: t.now,
		UID: owner[0], GID: owner[1], User: user, Group: group}
	switch e.Kind {
	case "dir":
		if base != nil && base.Type == data.NodeTypeDir {
			n := *base
			b.same[&n] = !b.policy(&n, e)
			return &n, nil
		}
		fresh.Type, fresh.Mode, fresh.Inode = data.NodeTypeDir, os.ModeDir|fresh.Mode, t.newInode()
		return fresh, nil
	case "symlink":
		if base != nil && base.Type == data.NodeTypeSymlink && symlinkTarget(base) == e.Target {
			n := *base
			b.same[&n] = !b.policy(&n, e)
			return &n, nil
		}
		fresh.Type, fresh.Mode, fresh.Inode, fresh.Links = data.NodeTypeSymlink, os.ModeSymlink|0777, t.newInode(), 1
		if utf8.ValidString(e.Target) {
			fresh.LinkTarget = e.Target
		} else {
			fresh.LinkTargetRaw = []byte(e.Target)
		}
		return fresh, nil
	}
	if keep {
		n := *base
		b.same[&n] = !b.policy(&n, e)
		return &n, nil
	}
	if tmpl := b.synth[e.LinkGroup]; e.LinkGroup != "" && tmpl != nil {
		n := *tmpl
		return &n, nil
	}
	content, err := b.content(ctx, e, base)
	if err != nil {
		return nil, err
	}
	fresh.Type, fresh.Inode, fresh.Links, fresh.Size, fresh.Content = data.NodeTypeFile, t.newInode(), 1, uint64(e.Size), content
	if e.LinkGroup != "" {
		fresh.Links, fresh.DeviceID = uint64(b.groupSize[e.LinkGroup]), device
		tmpl := *fresh
		b.synth[e.LinkGroup] = &tmpl
	}
	return fresh, nil
}

func symlinkTarget(n *data.Node) string {
	if n.LinkTargetRaw != nil {
		return string(n.LinkTargetRaw)
	}
	return n.LinkTarget
}

// content returns the blob IDs of an entry: an inline blob is chunked and
// saved; a restic token is found at the same path in the base or a source,
// else by a full walk of the sources, and every blob must be indexed with
// sizes that sum to the entry size.
func (b *mergeBuild) content(ctx context.Context, e *mergeEntry, base *data.Node) (restic.IDs, error) {
	if blob, ok := b.req.Blobs[e.Digest]; ok {
		sum := sha256.Sum256(blob)
		if hex.EncodeToString(sum[:]) != e.Digest || int64(len(blob)) != e.Size {
			return nil, invalidf("inline blob digest of %q", e.Path)
		}
		ids, _, err := b.s.hashContent(ctx, bytes.NewReader(blob))
		return ids, err
	}
	if !strings.HasPrefix(e.Digest, "restic:") {
		return nil, invalidf("no content for %q", e.Path)
	}
	var ids restic.IDs
	if base != nil && base.Type == data.NodeTypeFile && contentToken(base.Content) == e.Digest {
		ids = base.Content
	}
	for _, src := range b.sources {
		if ids != nil {
			break
		}
		var ino uint64
		n, err := b.s.newEditTree(src, time.Time{}, [2]uint32{}, &ino).node(ctx, e.Path)
		var refusal *writeRefusal
		if err != nil && !errors.As(err, &refusal) {
			return nil, err
		}
		if n != nil && n.Type == data.NodeTypeFile && contentToken(n.Content) == e.Digest {
			ids = n.Content
		}
	}
	for _, src := range b.sources {
		if ids != nil {
			break
		}
		tokens, err := b.s.tokenIndex(ctx, src)
		if err != nil {
			return nil, err
		}
		ids = tokens[e.Digest]
	}
	if ids == nil {
		return nil, invalidf("content of %q is in no source", e.Path)
	}
	var total uint64
	for _, id := range ids {
		n, ok := b.s.repo.LookupBlobSize(restic.DataBlob, id)
		if !ok {
			return nil, errors.New("source blob is not indexed")
		}
		total += uint64(n)
	}
	if total != uint64(e.Size) {
		return nil, invalidf("content size of %q", e.Path)
	}
	return append(restic.IDs{}, ids...), nil
}

// tokenIndex maps every file token below a tree to its blob IDs.
func (s *serveWriteHandler) tokenIndex(ctx context.Context, root restic.ID) (map[string]restic.IDs, error) {
	if idx, ok := s.tokens[root]; ok {
		return idx, nil
	}
	idx := map[string]restic.IDs{}
	var walk func(restic.ID) error
	walk = func(id restic.ID) error {
		nodes, err := s.loadNodes(ctx, id, false)
		if err != nil {
			return err
		}
		for _, n := range nodes {
			switch n.Type {
			case data.NodeTypeFile:
				idx[contentToken(n.Content)] = n.Content
			case data.NodeTypeDir:
				if err = walk(*n.Subtree); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return nil, err
	}
	if len(s.tokens) >= 8 {
		clear(s.tokens)
	}
	s.tokens[root] = idx
	return idx, nil
}
