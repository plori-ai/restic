package archiver

import (
	"context"
	"fmt"
	"math"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

// treeMetadata stages completed native nodes, never live stat information. A
// whole-tree inventory is necessary: hard links can cross subtree boundaries.
// Memory use is O(nodes); no unnormalized trees are uploaded by this path.
type treeMetadata struct {
	mu     sync.Mutex
	trees  map[string][]*data.Node
	parent map[string]*data.Node
}

func newTreeMetadata(ctx context.Context, loader restic.BlobLoader, sn *data.Snapshot) (*treeMetadata, error) {
	if sn.Tree == nil {
		return nil, fmt.Errorf("tree metadata parent has no root tree")
	}
	m := &treeMetadata{trees: make(map[string][]*data.Node), parent: make(map[string]*data.Node)}
	var load func(string, restic.ID) error
	load = func(dir string, id restic.ID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		tree, err := data.LoadTree(ctx, loader, id)
		if err != nil {
			return fmt.Errorf("load tree metadata parent at %s: %w", dir, err)
		}
		for item := range tree {
			if item.Error != nil {
				return item.Error
			}
			n := item.Node
			if n.Name == "" || n.Name == "." || n.Name == ".." || path.Base(n.Name) != n.Name {
				return fmt.Errorf("invalid tree metadata parent name %q", n.Name)
			}
			p := join(dir, n.Name)
			if m.parent[p] != nil {
				return fmt.Errorf("duplicate tree metadata parent path %s", p)
			}
			m.parent[p] = n
			if n.Type == data.NodeTypeDir {
				if n.Subtree == nil {
					return fmt.Errorf("tree metadata parent directory %s has no subtree", p)
				}
				if err := load(p, *n.Subtree); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := load("/", *sn.Tree); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *treeMetadata) stage(dir string, nodes []*data.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trees[dir] = nodes
}

// portableEqual also compares atime (when explicitly recorded), generic
// attributes and errors. Numeric ownership is authoritative; parent name and
// timestamp representations are retained by copying the complete parent node.
func portableEqual(a, b *data.Node) bool {
	if a == nil || b == nil || a.Error != "" || b.Error != "" {
		return false
	}
	x, y := *a, *b
	x.Inode, y.Inode, x.DeviceID, y.DeviceID = 0, 0, 0, 0
	x.ChangeTime, y.ChangeTime = time.Time{}, time.Time{}
	x.User, y.User, x.Group, y.Group = "", "", "", ""
	return x.Equals(y)
}

type metadataIdentity struct {
	inode, device uint64
	// Single-link files and zero identities must not alias unrelated paths.
	single string
}

func metadataGroups(nodes map[string]*data.Node) (map[string][]string, uint64, uint64) {
	groups := make(map[metadataIdentity][]string)
	var maxInode, maxDevice uint64
	for p, n := range nodes {
		maxInode = max(maxInode, n.Inode)
		maxDevice = max(maxDevice, n.DeviceID)
		if n.Type != data.NodeTypeFile {
			continue
		}
		id := metadataIdentity{inode: n.Inode, device: n.DeviceID}
		if n.Links <= 1 || (n.Inode == 0 && n.DeviceID == 0) {
			id.single = p
		}
		groups[id] = append(groups[id], p)
	}
	byPath := make(map[string][]string)
	for _, group := range groups {
		sort.Strings(group)
		for _, p := range group {
			byPath[p] = group
		}
	}
	return byPath, maxInode, maxDevice
}

func completeMetadataGroup(nodes map[string]*data.Node, group []string) bool {
	for _, p := range group {
		n := nodes[p]
		if n.Links != uint64(len(group)) || (n.Inode == 0 && n.DeviceID == 0) {
			return false
		}
	}
	return len(group) > 0
}

func equalMetadataGroup(live, parent map[string]*data.Node, group, old []string) bool {
	if len(group) != len(old) || !completeMetadataGroup(live, group) || !completeMetadataGroup(parent, old) {
		return false
	}
	for i, p := range group {
		if p != old[i] || !portableEqual(live[p], parent[p]) {
			return false
		}
	}
	return true
}

func metadataChangedTime(n, old *data.Node) error {
	if old != nil && n.Size == old.Size && n.ModTime.Equal(old.ModTime) && !n.ChangeTime.After(old.ChangeTime) {
		n.ChangeTime = old.ChangeTime.Add(time.Nanosecond)
		if n.ChangeTime.Year() > 9999 {
			return fmt.Errorf("tree metadata ctime overflow for %s", n.Name)
		}
	}
	return nil
}

func (m *treeMetadata) normalizeFiles() error {
	live := make(map[string]*data.Node)
	for dir, nodes := range m.trees {
		for _, n := range nodes {
			live[join(dir, n.Name)] = n
		}
	}
	groups, maxInode, maxDevice := metadataGroups(live)
	parents, parentInode, parentDevice := metadataGroups(m.parent)
	maxInode, maxDevice = max(maxInode, parentInode), max(maxDevice, parentDevice)
	paths := make([]string, 0, len(groups))
	for p := range groups {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		group := groups[p]
		if p != group[0] {
			continue
		}
		// Reject a changing/inconsistent hard-link group rather than restoring
		// one peer's bytes/metadata over another's. Name is not group metadata.
		first := *live[p]
		if first.Links > 1 && first.Inode == 0 && first.DeviceID == 0 {
			return fmt.Errorf("unidentifiable live hard-link group at %s", p)
		}
		for _, peer := range group[1:] {
			n := *live[peer]
			n.Name = first.Name
			if !portableEqual(&first, &n) {
				return fmt.Errorf("inconsistent live hard-link group at %s and %s", p, peer)
			}
		}
		if equalMetadataGroup(live, m.parent, group, parents[p]) {
			for _, peer := range group {
				*live[peer] = *m.parent[peer]
			}
			continue
		}
		// Every changed group gets a new namespace above both inventories.
		// Parent identities and independent equal-byte groups cannot collide.
		if maxInode == math.MaxUint64 || maxDevice == math.MaxUint64 {
			return fmt.Errorf("tree metadata hard-link identity overflow at %s", p)
		}
		maxInode++
		maxDevice++
		for _, peer := range group {
			n := live[peer]
			n.Inode, n.DeviceID = maxInode, maxDevice
			if err := metadataChangedTime(n, m.parent[peer]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *treeMetadata) finalize(ctx context.Context, saver restic.BlobSaver) (restic.ID, ItemStats, error) {
	var stats ItemStats
	if err := m.normalizeFiles(); err != nil {
		return restic.ID{}, stats, err
	}
	var save func(string) (restic.ID, error)
	save = func(dir string) (restic.ID, error) {
		if err := ctx.Err(); err != nil {
			return restic.ID{}, err
		}
		nodes, ok := m.trees[dir]
		if !ok {
			return restic.ID{}, fmt.Errorf("missing completed tree at %s", dir)
		}
		builder := data.NewTreeJSONBuilder()
		for _, n := range nodes {
			p := join(dir, n.Name)
			if n.Type == data.NodeTypeDir {
				id, err := save(p)
				if err != nil {
					return restic.ID{}, err
				}
				n.Subtree = &id
			}
			if n.Type != data.NodeTypeFile {
				old := m.parent[p]
				if portableEqual(n, old) {
					*n = *old
				} else if err := metadataChangedTime(n, old); err != nil {
					return restic.ID{}, err
				}
			}
			// This is the first serialization of each completed stored node.
			if err := builder.AddNode(n); err != nil {
				return restic.ID{}, err
			}
		}
		buf, err := builder.Finalize()
		if err != nil {
			return restic.ID{}, err
		}
		id, known, packed, err := saver.SaveBlob(ctx, restic.TreeBlob, buf, restic.ID{}, false)
		if err == nil && !known {
			stats.TreeBlobs++
			stats.TreeSize += uint64(len(buf))
			stats.TreeSizeInRepo += uint64(packed)
		}
		return id, err
	}
	id, err := save("/")
	return id, stats, err
}
