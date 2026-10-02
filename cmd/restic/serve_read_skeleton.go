package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/sync/errgroup"
)

// skeleton streams the metadata a lazy working copy needs to build its tree
// without content: one compact JSON object per line, in walk order (parents
// before children, names sorted). Content and subtree IDs, ctime, user and group
// names are omitted. Trees are loaded concurrently (one level at a time) and the
// records are written through a buffered writer; the /walk endpoint loads trees
// one at a time and encodes complete nodes.
//
// Record fields: p path, t type (d,f,l,o), m unix mode bits (permissions,
// setuid/setgid/sticky), u uid, g gid, s size, mt/at mtime/atime in ns, n links,
// i inode, v device id (only when n > 1, for hard-link grouping), l symlink
// target, x extended attributes [[name, base64 value]].
func (s *serveReadHandler) skeleton(ctx context.Context, w http.ResponseWriter, root restic.ID) error {
	trees := map[restic.ID][]*data.Node{}
	level := restic.IDs{root}
	for len(level) > 0 {
		var mu sync.Mutex
		var next restic.IDs
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(8)
		for _, id := range level {
			g.Go(func() error {
				it, err := data.LoadTree(gctx, s.repo, id)
				if err != nil {
					return err
				}
				var nodes []*data.Node
				var subs restic.IDs
				for item := range it {
					if item.Error != nil {
						return item.Error
					}
					nodes = append(nodes, item.Node)
					if item.Node.Type == data.NodeTypeDir && item.Node.Subtree != nil {
						subs = append(subs, *item.Node.Subtree)
					}
				}
				mu.Lock()
				trees[id] = nodes
				next = append(next, subs...)
				mu.Unlock()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		level = next[:0:0]
		for _, id := range next {
			if _, ok := trees[id]; !ok {
				level = append(level, id)
			}
		}
	}

	bw := bufio.NewWriterSize(w, 1<<20)
	buf := make([]byte, 0, 4096)
	var emit func(dir string, id restic.ID) error
	emit = func(dir string, id restic.ID) error {
		for _, n := range trees[id] {
			p := path.Join(dir, n.Name)
			buf = appendSkeletonRecord(buf[:0], p, n)
			if _, err := bw.Write(buf); err != nil {
				return err
			}
			if n.Type == data.NodeTypeDir && n.Subtree != nil {
				if err := emit(p, *n.Subtree); err != nil {
					return err
				}
			}
		}
		return ctx.Err()
	}
	if err := emit("/", root); err != nil {
		return err
	}
	return bw.Flush()
}

func appendSkeletonRecord(b []byte, p string, n *data.Node) []byte {
	b = append(b, `{"p":`...)
	b = appendJSONString(b, p)
	t := "o"
	switch n.Type {
	case data.NodeTypeDir:
		t = "d"
	case data.NodeTypeFile:
		t = "f"
	case data.NodeTypeSymlink:
		t = "l"
	}
	b = append(b, `,"t":"`...)
	b = append(b, t...)
	b = append(b, `","m":`...)
	m := uint64(n.Mode.Perm())
	if n.Mode&os.ModeSetuid != 0 {
		m |= 0o4000
	}
	if n.Mode&os.ModeSetgid != 0 {
		m |= 0o2000
	}
	if n.Mode&os.ModeSticky != 0 {
		m |= 0o1000
	}
	b = strconv.AppendUint(b, m, 10)
	b = append(b, `,"u":`...)
	b = strconv.AppendUint(b, uint64(n.UID), 10)
	b = append(b, `,"g":`...)
	b = strconv.AppendUint(b, uint64(n.GID), 10)
	b = append(b, `,"s":`...)
	b = strconv.AppendUint(b, n.Size, 10)
	b = append(b, `,"mt":`...)
	b = strconv.AppendInt(b, unixNano(n.ModTime), 10)
	b = append(b, `,"at":`...)
	b = strconv.AppendInt(b, unixNano(n.AccessTime), 10)
	if n.Links > 1 && n.Type != data.NodeTypeDir {
		b = append(b, `,"n":`...)
		b = strconv.AppendUint(b, n.Links, 10)
		b = append(b, `,"i":`...)
		b = strconv.AppendUint(b, n.Inode, 10)
		b = append(b, `,"v":`...)
		b = strconv.AppendUint(b, n.DeviceID, 10)
	}
	if n.Type == data.NodeTypeSymlink {
		b = append(b, `,"l":`...)
		b = appendJSONString(b, n.LinkTarget)
	}
	if len(n.ExtendedAttributes) > 0 {
		b = append(b, `,"x":[`...)
		for i, x := range n.ExtendedAttributes {
			if i > 0 {
				b = append(b, ',')
			}
			b = append(b, '[')
			b = appendJSONString(b, x.Name)
			b = append(b, ',', '"')
			b = base64.StdEncoding.AppendEncode(b, x.Value)
			b = append(b, '"', ']')
		}
		b = append(b, ']')
	}
	return append(b, '}', '\n')
}

// appendJSONString appends s as a JSON string. Plain printable ASCII takes the
// fast path; anything else goes through encoding/json (which replaces invalid
// UTF-8, as restic's own JSON output does).
func appendJSONString(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c >= 0x7f || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			q, _ := json.Marshal(s)
			return append(b, q...)
		}
	}
	b = append(b, '"')
	b = append(b, s...)
	return append(b, '"')
}

// unixNano is t in ns since the epoch; 0 for an unset time (UnixNano of the
// zero time is undefined).
func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}
