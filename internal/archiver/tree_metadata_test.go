package archiver

import (
	"context"
	"encoding/json"
	"math"
	"path"
	"testing"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func metadataFixture() *treeMetadata {
	mtime := time.Unix(1700000000, 123456789)
	base := data.Node{Type: data.NodeTypeFile, Mode: 0640, ModTime: mtime, AccessTime: mtime,
		ChangeTime: mtime.Add(time.Second), UID: 1000, GID: 1000, User: "original", Group: "original",
		Inode: 10, DeviceID: 20, Links: 2, Size: 4, Content: restic.IDs{restic.Hash([]byte("data"))},
		ExtendedAttributes: []data.ExtendedAttribute{{Name: "user.test", Value: []byte("value")}}}
	m := &treeMetadata{trees: make(map[string][]*data.Node), parent: make(map[string]*data.Node)}
	for _, p := range []string{"/a", "/sub/b", "/c", "/sub/d"} {
		n := base
		if p == "/c" || p == "/sub/d" {
			n.Inode++ // Equal bytes, distinct hard-link partition.
		}
		n.Name = path.Base(p)
		m.parent[p] = &n
		live := n
		live.Inode += 100
		live.DeviceID += 200
		live.ChangeTime = mtime.Add(5 * time.Second)
		live.User, live.Group = "renamed", "renamed"
		live.ModTime = mtime.In(time.FixedZone("representation", 3600))
		dir := "/"
		if p == "/sub/b" || p == "/sub/d" {
			dir = "/sub"
		}
		m.trees[dir] = append(m.trees[dir], &live)
	}
	return m
}

func TestTreeMetadataCompleteGroups(t *testing.T) {
	m := metadataFixture()
	rtest.OK(t, m.normalizeFiles())
	for dir, nodes := range m.trees {
		for _, n := range nodes {
			old := m.parent[join(dir, n.Name)]
			a, err := json.Marshal(n)
			rtest.OK(t, err)
			b, err := json.Marshal(old)
			rtest.OK(t, err)
			rtest.Equals(t, string(b), string(a))
		}
	}
	rtest.Assert(t, m.trees["/"][0].Inode != m.trees["/"][1].Inode, "equal-byte groups merged")
}

func TestTreeMetadataPortableChanges(t *testing.T) {
	tests := map[string]func(*data.Node){
		"mode":             func(n *data.Node) { n.Mode ^= 0100 },
		"uid":              func(n *data.Node) { n.UID++ },
		"gid":              func(n *data.Node) { n.GID++ },
		"mtime-nanosecond": func(n *data.Node) { n.ModTime = n.ModTime.Add(time.Nanosecond) },
		"atime":            func(n *data.Node) { n.AccessTime = n.AccessTime.Add(time.Nanosecond) },
		"xattr": func(n *data.Node) {
			n.ExtendedAttributes = []data.ExtendedAttribute{{Name: "user.test", Value: []byte("edit")}}
		},
		"generic-attribute": func(n *data.Node) {
			n.GenericAttributes = map[data.GenericAttributeType]json.RawMessage{"test": json.RawMessage(`true`)}
		},
		"symlink": func(n *data.Node) { n.LinkTarget = "new target" },
		"rdev":    func(n *data.Node) { n.Device++ },
		"size":    func(n *data.Node) { n.Size++ },
		"links":   func(n *data.Node) { n.Links++ },
		"content": func(n *data.Node) { n.Content = restic.IDs{restic.Hash([]byte("edit"))} },
		"error":   func(n *data.Node) { n.Error = "incomplete metadata" },
	}
	for name, edit := range tests {
		t.Run(name, func(t *testing.T) {
			m := metadataFixture()
			edit(m.trees["/"][0])
			edit(m.trees["/sub"][0])
			err := m.normalizeFiles()
			if name == "error" {
				rtest.Assert(t, err != nil, "incomplete metadata group accepted")
				return
			}
			rtest.OK(t, err)
			n := m.trees["/"][0]
			rtest.Assert(t, n.Inode > 111 && n.DeviceID > 220, "changed identity not above inventories: %v", n)
			rtest.Equals(t, n.Inode, m.trees["/sub"][0].Inode)
			rtest.Equals(t, m.parent["/c"].Inode, m.trees["/"][1].Inode)
		})
	}
}

func TestTreeMetadataTopology(t *testing.T) {
	for _, test := range []string{"split", "join", "remove", "incomplete", "zero", "inconsistent"} {
		t.Run(test, func(t *testing.T) {
			m := metadataFixture()
			switch test {
			case "split":
				m.trees["/"][0].Links = 1
				m.trees["/sub"][0].Links = 1
				m.trees["/sub"][0].Inode++
			case "join":
				for _, nodes := range m.trees {
					for _, n := range nodes {
						n.Inode, n.Links = 110, 4
					}
				}
			case "remove":
				m.trees["/sub"] = m.trees["/sub"][1:]
				m.trees["/"][0].Links = 1
			case "incomplete":
				m.trees["/sub"] = m.trees["/sub"][1:]
			case "zero":
				for _, nodes := range m.trees {
					for _, n := range nodes {
						n.Inode, n.DeviceID = 0, 0
					}
				}
			case "inconsistent":
				m.trees["/sub"][0].Content = restic.IDs{restic.Hash([]byte("race"))}
			}
			err := m.normalizeFiles()
			if test == "inconsistent" || test == "zero" {
				rtest.Assert(t, err != nil, "unsafe group accepted")
				return
			}
			rtest.OK(t, err)
			a := m.trees["/"][0]
			rtest.Assert(t, a.Inode != m.parent["/a"].Inode, "changed topology reused parent")
			rtest.Assert(t, a.Inode != 0 || a.DeviceID != 0, "zero group")
			if test == "split" || test == "zero" {
				rtest.Assert(t, a.Inode != m.trees["/sub"][0].Inode, "split groups collided")
			}
			if test == "join" {
				for _, nodes := range m.trees {
					for _, n := range nodes {
						rtest.Equals(t, a.Inode, n.Inode)
					}
				}
			}
		})
	}
}

func TestTreeMetadataOverflowAndChangedETag(t *testing.T) {
	for _, field := range []string{"inode", "device"} {
		t.Run(field, func(t *testing.T) {
			m := metadataFixture()
			if field == "inode" {
				m.parent["/a"].Inode = math.MaxUint64
			} else {
				m.parent["/a"].DeviceID = math.MaxUint64
			}
			rtest.Assert(t, m.normalizeFiles() != nil, "identity overflow accepted")
		})
	}
	n := &data.Node{Name: "file", Size: 4, ModTime: time.Unix(1, 1), ChangeTime: time.Unix(2, 1)}
	old := *n
	rtest.OK(t, metadataChangedTime(n, &old))
	rtest.Equals(t, old.ChangeTime.Add(time.Nanosecond), n.ChangeTime)
	old.ChangeTime = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	rtest.Assert(t, metadataChangedTime(n, &old) != nil, "ctime overflow accepted")
}

func TestTreeMetadataParentErrors(t *testing.T) {
	repo := repository.TestRepository(t)
	_, err := newTreeMetadata(context.Background(), repo, &data.Snapshot{})
	rtest.Assert(t, err != nil, "missing root accepted")
	id := restic.Hash([]byte("missing"))
	_, err = newTreeMetadata(context.Background(), repo, &data.Snapshot{Tree: &id})
	rtest.Assert(t, err != nil, "unreadable parent accepted")
}
