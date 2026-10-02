package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

// Metadata stream of the lazyfill protocol, version 1 (doc/plori-lazy-fill.md):
// a framed binary description of one snapshot's tree that a skeleton builder
// turns into a native tree without file content.
const (
	lzfmMagic = "LZFM"
	// lzfmMaxFrame bounds a frame payload.
	lzfmMaxFrame = 16 << 20
	// Linux and ext4 limits that the protocol enforces.
	lzfmMaxPath       = 4095
	lzfmMaxName       = 255
	lzfmMaxLinkTarget = 4095
	lzfmMaxXattrName  = 255
	lzfmMaxXattrValue = 65536
)

// Record types of the metadata stream.
const (
	lzfmDir      = 1
	lzfmFile     = 2
	lzfmSymlink  = 3
	lzfmFifo     = 4
	lzfmCharDev  = 5
	lzfmBlockDev = 6
	lzfmSocket   = 7
)

const (
	lzfmFrameHeader  = 'H'
	lzfmFrameRecord  = 'R'
	lzfmFrameTrailer = 'T'
)

// lzfmEncoder writes the framing: magic, version, frames of kind, uvarint
// length and payload. The trailer digest is SHA-256 over the header and
// record frames exactly as written.
type lzfmEncoder struct {
	w      *bufio.Writer
	h      hash.Hash
	f      []byte // payload under construction
	s      []byte // value of a composite field under construction
	last   uint64 // last field tag of f
	counts lzfmCounts
}

type lzfmCounts struct {
	records, dirs, files, symlinks, others, fileBytes uint64
}

func newLZFMEncoder(w io.Writer) *lzfmEncoder { return newLZFEncoder(w, lzfmMagic) }

// newLZFEncoder starts a stream of any lazyfill format, version 1.
func newLZFEncoder(w io.Writer, magic string) *lzfmEncoder {
	e := &lzfmEncoder{w: bufio.NewWriterSize(w, 1<<20), h: sha256.New()}
	_, _ = e.w.WriteString(magic)
	_, _ = e.w.Write(binary.AppendUvarint(nil, 1))
	return e
}

func (e *lzfmEncoder) reset() { e.f, e.last = e.f[:0], 0 }

func (e *lzfmEncoder) tag(t uint64) {
	if t < e.last {
		panic(fmt.Sprintf("lzfm: field tag %d after %d", t, e.last))
	}
	e.last = t
	e.f = binary.AppendUvarint(e.f, t)
}

func (e *lzfmEncoder) uint(t, v uint64) {
	e.tag(t)
	e.f = binary.AppendUvarint(e.f, v)
}

func (e *lzfmEncoder) sint(t uint64, v int64) {
	e.tag(t)
	e.f = binary.AppendVarint(e.f, v)
}

func (e *lzfmEncoder) bytes(t uint64, v []byte) {
	e.tag(t)
	e.f = binary.AppendUvarint(e.f, uint64(len(v)))
	e.f = append(e.f, v...)
}

// xattr and blob write one bytes field whose value holds two
// length-prefixed parts: an extended attribute's name and value, or a blob's
// ID and plaintext length.
func (e *lzfmEncoder) xattr(t uint64, name string, value []byte) {
	e.s = binary.AppendUvarint(e.s[:0], uint64(len(name)))
	e.s = append(e.s, name...)
	e.s = binary.AppendUvarint(e.s, uint64(len(value)))
	e.s = append(e.s, value...)
	e.bytes(t, e.s)
}

func (e *lzfmEncoder) blob(t uint64, id restic.ID, length uint64) {
	e.s = binary.AppendUvarint(e.s[:0], uint64(len(id)))
	e.s = append(e.s, id[:]...)
	e.s = binary.AppendUvarint(e.s, length)
	e.bytes(t, e.s)
}

func (e *lzfmEncoder) frame(kind byte) error {
	if len(e.f) > lzfmMaxFrame {
		return fmt.Errorf("frame of %d bytes exceeds %d", len(e.f), lzfmMaxFrame)
	}
	prefix := binary.AppendUvarint([]byte{kind}, uint64(len(e.f)))
	if kind != lzfmFrameTrailer {
		e.h.Write(prefix)
		e.h.Write(e.f)
	}
	if _, err := e.w.Write(prefix); err != nil {
		return err
	}
	_, err := e.w.Write(e.f)
	return err
}

// header writes the header frame: source (snapshot ID), root tree ID and the
// producer text.
func (e *lzfmEncoder) header(source, root restic.ID, producer string) error {
	e.reset()
	e.bytes(1, source[:])
	e.bytes(2, root[:])
	e.bytes(3, []byte(producer))
	return e.frame(lzfmFrameHeader)
}

// lzfmRecord is one tree entry in wire form.
type lzfmRecord struct {
	path       []byte
	typ        uint64
	mode       uint64
	uid, gid   uint64
	mtime      int64
	atime      int64
	size       uint64
	devMajor   uint64
	devMinor   uint64
	linkTarget []byte
	linkGroup  uint64
	linkCount  uint64
	xattrs     []data.ExtendedAttribute
	blobs      []restic.ID
	blobSizes  []uint64
}

func (e *lzfmEncoder) record(r *lzfmRecord) error {
	e.reset()
	e.bytes(1, r.path)
	e.uint(2, r.typ)
	e.uint(3, r.mode)
	e.uint(4, r.uid)
	e.uint(5, r.gid)
	e.sint(6, r.mtime)
	e.sint(7, r.atime)
	if r.typ == lzfmFile {
		e.uint(8, r.size)
	}
	if r.typ == lzfmCharDev || r.typ == lzfmBlockDev {
		e.uint(9, r.devMajor)
		e.uint(10, r.devMinor)
	}
	if r.typ == lzfmSymlink {
		e.bytes(11, r.linkTarget)
	}
	if r.linkGroup != 0 {
		e.uint(12, r.linkGroup)
		e.uint(13, r.linkCount)
	}
	for _, x := range r.xattrs {
		e.xattr(14, x.Name, x.Value)
	}
	for i, id := range r.blobs {
		e.blob(15, id, r.blobSizes[i])
	}
	if err := e.frame(lzfmFrameRecord); err != nil {
		return err
	}
	c := &e.counts
	c.records++
	switch r.typ {
	case lzfmDir:
		c.dirs++
	case lzfmFile:
		c.files++
		c.fileBytes += r.size
	case lzfmSymlink:
		c.symlinks++
	default:
		c.others++
	}
	return nil
}

// close writes the completion trailer: the counts and the digest.
func (e *lzfmEncoder) close() error {
	e.reset()
	c := e.counts
	e.uint(1, c.records)
	e.uint(2, c.dirs)
	e.uint(3, c.files)
	e.uint(4, c.symlinks)
	e.uint(5, c.others)
	e.uint(6, c.fileBytes)
	e.bytes(7, e.h.Sum(nil))
	if err := e.frame(lzfmFrameTrailer); err != nil {
		return err
	}
	return e.w.Flush()
}

// fail writes an error trailer, so a consumer that read records learns that
// the stream is not a complete description.
func (e *lzfmEncoder) fail(msg string) error {
	e.reset()
	e.bytes(8, []byte(msg))
	if err := e.frame(lzfmFrameTrailer); err != nil {
		return err
	}
	return e.w.Flush()
}

// skeletonError is a snapshot that the metadata stream cannot describe
// losslessly. It ends the stream with an error trailer.
type skeletonError struct{ msg string }

func (e *skeletonError) Error() string { return e.msg }

func skeletonErrorf(format string, args ...any) error {
	return &skeletonError{msg: fmt.Sprintf(format, args...)}
}

// checkRawName checks one path component: 1..255 bytes, no '/' or NUL, not
// "." or "..".
func checkRawName(name string) error {
	if name == "" || name == "." || name == ".." || len(name) > lzfmMaxName || strings.ContainsAny(name, "/\x00") {
		return fmt.Errorf("name %q is not a valid path component", name)
	}
	return nil
}

// checkRawPath checks a relative path: components joined by '/', at most
// 4095 bytes. The empty path is the root.
func checkRawPath(p []byte) error {
	if len(p) > lzfmMaxPath {
		return fmt.Errorf("path of %d bytes", len(p))
	}
	if len(p) == 0 {
		return nil
	}
	for c := range bytes.SplitSeq(p, []byte("/")) {
		if err := checkRawName(string(c)); err != nil {
			return err
		}
	}
	return nil
}

// lzfmDevice splits a Linux dev_t (stat.st_rdev, as restic stores it for
// device nodes) into major and minor numbers, as unix.Major and unix.Minor do
// on Linux. Snapshots of workspaces come from Linux hosts; the formula does
// not depend on the platform this process runs on.
func lzfmDevice(dev uint64) (major, minor uint64) {
	major = (dev&0x00000000000fff00)>>8 | (dev&0xfffff00000000000)>>32
	minor = dev&0x00000000000000ff | (dev&0x00000ffffff00000)>>12
	return major, minor
}

// lzfmMode is the permission bits plus setuid, setgid and sticky.
func lzfmMode(m os.FileMode) uint64 {
	mode := uint64(m.Perm())
	if m&os.ModeSetuid != 0 {
		mode |= 0o4000
	}
	if m&os.ModeSetgid != 0 {
		mode |= 0o2000
	}
	if m&os.ModeSticky != 0 {
		mode |= 0o1000
	}
	return mode
}

var lzfmTypes = map[data.NodeType]uint64{
	data.NodeTypeDir: lzfmDir, data.NodeTypeFile: lzfmFile, data.NodeTypeSymlink: lzfmSymlink, data.NodeTypeFifo: lzfmFifo,
	data.NodeTypeCharDev: lzfmCharDev, data.NodeTypeDev: lzfmBlockDev, data.NodeTypeSocket: lzfmSocket,
}

// skeletonWalk produces the records of one snapshot tree in walk order: each
// directory before its entries, entries in the tree's (name) order. While the
// walk emits a directory, the trees of its next skeletonPrefetch
// subdirectories load in the background, at most skeletonLoads at once per
// request; without a repository cache every tree is one backend read, so
// loading them one at a time would add one round trip per directory. Memory
// is bounded by the depth of the tree times skeletonPrefetch+1 directories,
// plus one number per hard-linked inode.
type skeletonWalk struct {
	ctx    context.Context
	loader restic.BlobLoader
	slots  chan struct{}
	sizes  func(restic.IDs) ([]uint64, error)
	emit   func(*lzfmRecord) error
	groups map[[2]uint64]uint64
	rec    lzfmRecord
}

const (
	skeletonPrefetch = 8
	skeletonLoads    = 8
)

// treeLoad is one tree loading in the background.
type treeLoad struct {
	done  chan struct{}
	nodes []*data.Node
	err   error
}

func (w *skeletonWalk) load(id restic.ID) *treeLoad {
	l := &treeLoad{done: make(chan struct{})}
	go func() {
		defer close(l.done)
		select {
		case w.slots <- struct{}{}:
			defer func() { <-w.slots }()
		case <-w.ctx.Done():
			l.err = w.ctx.Err()
			return
		}
		nodes, err := data.LoadTree(w.ctx, w.loader, id)
		if err != nil {
			l.err = err
			return
		}
		for item := range nodes {
			if item.Error != nil {
				l.err = item.Error
				return
			}
			l.nodes = append(l.nodes, item.Node)
		}
	}()
	return l
}

func (w *skeletonWalk) tree(dir []byte, l *treeLoad) error {
	select {
	case <-l.done:
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
	if l.err != nil {
		return l.err
	}
	var subdirs []int // positions of the directories that have a subtree
	for i, n := range l.nodes {
		if n.Type == data.NodeTypeDir && n.Subtree != nil {
			subdirs = append(subdirs, i)
		}
	}
	loads := map[int]*treeLoad{}
	started, entered := 0, 0
	prefetch := func() {
		for started < len(subdirs) && started <= entered+skeletonPrefetch {
			i := subdirs[started]
			loads[i] = w.load(*l.nodes[i].Subtree)
			started++
		}
	}
	prefetch()
	for i, n := range l.nodes {
		if err := checkRawName(n.Name); err != nil {
			return skeletonErrorf("in directory %q: %v", "/"+string(dir), err)
		}
		// Paths share one backing array, used as a stack: a path is written
		// over its predecessor's after that one and its subtree were
		// emitted, and the encoder copies it.
		p := dir
		if len(p) > 0 {
			p = append(p, '/')
		}
		p = append(p, n.Name...)
		if len(p) > lzfmMaxPath {
			return skeletonErrorf("path %q is longer than %d bytes", p, lzfmMaxPath)
		}
		if err := w.node(p, n); err != nil {
			return err
		}
		if n.Type == data.NodeTypeDir {
			sub := loads[i]
			delete(loads, i)
			entered++
			prefetch()
			if err := w.tree(p, sub); err != nil {
				return err
			}
		}
	}
	return w.ctx.Err()
}

// node maps one restic node to a record and emits it.
func (w *skeletonWalk) node(p []byte, n *data.Node) error {
	typ, ok := lzfmTypes[n.Type]
	if !ok {
		return skeletonErrorf("%q has node type %q, which the metadata stream does not describe", p, n.Type)
	}
	r := &w.rec
	*r = lzfmRecord{path: p, typ: typ, mode: lzfmMode(n.Mode), uid: uint64(n.UID), gid: uint64(n.GID),
		// restic restore passes exactly these values to utimensat.
		mtime: n.ModTime.UnixNano(), atime: n.AccessTime.UnixNano(), xattrs: n.ExtendedAttributes, blobs: n.Content}
	switch n.Type {
	case data.NodeTypeDir:
		if n.Subtree == nil {
			return skeletonErrorf("directory %q has no subtree", p)
		}
	case data.NodeTypeFile:
		r.size = n.Size
		sizes, err := w.sizes(n.Content)
		if err != nil {
			return err
		}
		var sum uint64
		for _, s := range sizes {
			sum += s
		}
		if sum != n.Size {
			return skeletonErrorf("%q: content blobs sum to %d bytes, size is %d", p, sum, n.Size)
		}
		r.blobSizes = sizes
	case data.NodeTypeSymlink:
		r.linkTarget = []byte(n.LinkTarget)
		if len(r.linkTarget) == 0 || len(r.linkTarget) > lzfmMaxLinkTarget || bytes.IndexByte(r.linkTarget, 0) >= 0 {
			return skeletonErrorf("symlink %q has a target of %d bytes or a NUL byte", p, len(r.linkTarget))
		}
	case data.NodeTypeCharDev, data.NodeTypeDev:
		r.devMajor, r.devMinor = lzfmDevice(n.Device)
	}
	if n.Type != data.NodeTypeFile && len(n.Content) > 0 {
		return skeletonErrorf("%q is a %s with content", p, n.Type)
	}
	if n.Type != data.NodeTypeDir && n.Links > 1 {
		key := [2]uint64{n.DeviceID, n.Inode}
		g, ok := w.groups[key]
		if !ok {
			g = uint64(len(w.groups)) + 1
			w.groups[key] = g
		}
		r.linkGroup, r.linkCount = g, n.Links
	}
	for _, x := range n.ExtendedAttributes {
		if x.Name == "" || len(x.Name) > lzfmMaxXattrName || strings.IndexByte(x.Name, 0) >= 0 || len(x.Value) > lzfmMaxXattrValue {
			return skeletonErrorf("%q has an extended attribute %q of %d bytes outside the protocol limits", p, x.Name, len(x.Value))
		}
	}
	return w.emit(r)
}

// serveSkeleton handles GET /skeleton?snapshot=ID. It answers 400, 404 or 500
// before the stream starts; a failure after that ends the stream with an
// error trailer frame.
func (s *serveReadHandler) serveSkeleton(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		serveReadError(w, http.StatusMethodNotAllowed)
		return
	}
	selector := r.URL.Query().Get("snapshot")
	id, err := restic.ParseID(selector)
	if err != nil || selector != id.String() || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		serveReadError(w, http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	root, err := s.sourceRoot(ctx, id)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		code := http.StatusInternalServerError
		if errors.Is(err, os.ErrNotExist) {
			code = http.StatusNotFound
		}
		serveReadError(w, code)
		return
	}
	w.Header().Set("Content-Type", "application/x-lazyfill-meta")
	enc := newLZFMEncoder(w)
	if err := enc.header(id, root, "restic "+global.Version+" serve-read /skeleton"); err != nil {
		return
	}
	// Background tree loads end with the request.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	walk := &skeletonWalk{ctx: ctx, loader: directLoader{s}, slots: make(chan struct{}, skeletonLoads), emit: enc.record, groups: map[[2]uint64]uint64{},
		sizes: func(ids restic.IDs) ([]uint64, error) {
			sizes := make([]uint64, len(ids))
			err := s.withIndexRetry(ctx, func(repo *repository.Repository) error {
				for i, id := range ids {
					n, ok := repo.LookupBlobSize(restic.DataBlob, id)
					if !ok {
						return skeletonErrorf("content blob %s is not in the repository index", id)
					}
					sizes[i] = uint64(n)
				}
				return nil
			})
			return sizes, err
		}}
	if err = walk.tree(nil, walk.load(root)); err == nil {
		err = enc.close()
	}
	if err != nil && ctx.Err() == nil {
		var se *skeletonError
		msg := "reading the snapshot failed"
		if errors.As(err, &se) {
			msg = se.msg
		}
		_, _ = fmt.Fprintf(os.Stderr, "serve-read: skeleton %s: %v\n", id.Str(), err)
		_ = enc.fail(msg)
	}
}
