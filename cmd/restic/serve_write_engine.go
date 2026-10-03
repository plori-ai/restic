package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/bloblru"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/walker"
)

const serveWriteStatsLimit = 1 << 21

// serveWriteHandler is one write engine over one repository handle. The
// server keeps two: the writer, which also serves reads, and the verifier,
// which replays a request against a separate read-only handle.
type serveWriteHandler struct {
	*serveReadHandler
	cfg     serveWriteConfig
	stats   map[restic.ID]*treeStats
	statsMu sync.Mutex
	walkers chan struct{}
	public  *walker.TreeRewriter
	// pending and pendingData hold the tree and data blobs of the current
	// request. A request computes its whole result before it saves anything,
	// so a refusal or a malformed edit leaves the repository untouched.
	pending     map[restic.ID][]byte
	pendingData map[restic.ID][]byte
	tokens      map[restic.ID]map[string]restic.IDs
	// snapshotsSeen and snapshotIDs keep loaded and written snapshots.
	snapshotsSeen map[restic.ID]*data.Snapshot
	snapshotIDs   map[*data.Snapshot]restic.ID
	// indexFiles is the index file listing of the last write request; a
	// file that disappeared means a prune ran, see refreshIndex.
	indexFiles restic.IDSet

	// verifier keeps every tree a replay encodes pending, so that the
	// re-read loads the candidate's trees from the backend; checkIndex
	// requires every referenced blob to be in the index; bypass lists trees
	// that are loaded from the backend without the blob cache.
	verifier bool
	bypass   restic.IDSet
	added    addedStats
}

type addedStats struct {
	dataAdded, dataAddedPacked uint64
	dataBlobs, treeBlobs       int
}

func newServeWriteHandler(repo *repository.Repository, cfg serveWriteConfig) *serveWriteHandler {
	s := &serveWriteHandler{serveReadHandler: newServeReadHandler(repo), cfg: cfg, walkers: make(chan struct{}, 8)}
	s.resetState()
	return s
}

// reset replaces the repository handle, for example after a failed upload,
// and drops every cache that could describe objects of the old handle.
func (s *serveWriteHandler) reset(repo *repository.Repository) {
	s.repoMu.Lock()
	s.repo = repo
	s.repoMu.Unlock()
	s.cache = bloblru.New(64 << 20)
	s.indexFiles = nil
	s.resetState()
}

func (s *serveWriteHandler) resetState() {
	s.resetStats()
	s.resetCall()
}

func (s *serveWriteHandler) resetStats() {
	s.statsMu.Lock()
	s.stats = map[restic.ID]*treeStats{}
	s.statsMu.Unlock()
}

// resetCall drops everything except the tree statistics and the blob cache.
func (s *serveWriteHandler) resetCall() {
	s.roots = map[restic.ID]restic.ID{}
	s.pending, s.pendingData = map[restic.ID][]byte{}, map[restic.ID][]byte{}
	s.tokens = map[restic.ID]map[string]restic.IDs{}
	s.snapshotsSeen, s.snapshotIDs = map[restic.ID]*data.Snapshot{}, map[*data.Snapshot]restic.ID{}
	s.bypass = restic.NewIDSet()
	s.resetProjection()
}

// resetProjection starts a new public projection. Its node cache maps a tree
// ID to the ID of the tree without private names; the projection depends only
// on the tree, so the cache serves every later head.
func (s *serveWriteHandler) resetProjection() {
	s.public = walker.NewTreeRewriter(walker.RewriteOpts{RewriteNode: func(n *data.Node, _ string) *data.Node {
		if s.excluded(n.Name) {
			return nil
		}
		return n
	}, KeepSubtree: func(id restic.ID, _ string) bool {
		// A subtree without a private name at any depth projects to itself.
		st, ok := s.cachedStats(id)
		return ok && st.entries == st.pubEntries
	}})
}

// refreshIndex makes the handle's index match the index files the backend
// lists now. Index files are immutable, so an unchanged listing needs no load;
// otherwise upstream's incremental load reads the new files, or reloads
// everything when a file vanished. A vanished file means a prune replaced
// index files: trees and blobs the caches remember may be gone, so the
// projection cache, the token index and loaded snapshots are dropped. Tree
// statistics describe immutable content and stay.
func (s *serveWriteHandler) refreshIndex(ctx context.Context, marks map[string]float64) error {
	start := time.Now()
	files, err := s.listIndex(ctx)
	if err != nil {
		return err
	}
	marks["index_list"] = since(start)
	changed := s.indexFiles == nil || len(files) != len(s.indexFiles)
	removed := false
	for id := range s.indexFiles {
		if !files.Has(id) {
			changed, removed = true, true
			break
		}
	}
	if removed {
		if s.verifier {
			// The verifier's statistics also certify that every blob below
			// a tree was in its index; that holds only while no index file
			// was removed.
			s.resetStats()
		}
		s.resetProjection()
		s.tokens = map[restic.ID]map[string]restic.IDs{}
		s.snapshotsSeen, s.snapshotIDs = map[restic.ID]*data.Snapshot{}, map[*data.Snapshot]restic.ID{}
		s.roots = map[restic.ID]restic.ID{}
	}
	if changed {
		start = time.Now()
		if err = s.repo.LoadIndex(ctx, nil); err != nil {
			s.indexFiles = nil
			return err
		}
		marks["index_load"] = since(start)
	}
	s.indexFiles = files
	return nil
}

// rememberIndex records the listing after this handle wrote its own index
// files, which its index already holds. A file another writer saved in the
// meantime is then loaded only at the next change of the listing; until then
// its blobs may be uploaded again (deduplication only), and a tree read falls
// back to serve-read's index refresh.
func (s *serveWriteHandler) rememberIndex(ctx context.Context) {
	files, err := s.listIndex(ctx)
	if err != nil {
		files = nil // load at the next request
	}
	s.indexFiles = files
}

func (s *serveWriteHandler) listIndex(ctx context.Context) (restic.IDSet, error) {
	files := restic.NewIDSet()
	err := s.repo.List(ctx, restic.IndexFile, func(id restic.ID, _ int64) error { files.Insert(id); return nil })
	return files, err
}

// LoadBlob serves trees this request encoded but has not saved yet. The
// verifier loads the candidate's new trees directly from the backend.
func (s *serveWriteHandler) LoadBlob(ctx context.Context, typ restic.BlobType, id restic.ID, buf []byte) ([]byte, error) {
	if typ == restic.TreeBlob {
		if b, ok := s.pending[id]; ok {
			return b, nil
		}
		// bypass is only read while walkers run.
		if s.bypass.Has(id) {
			b, err := s.repo.LoadBlob(ctx, typ, id, nil)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, &verifyFailure{Code: "tree_unreadable", Detail: "tree " + id.Str()}
			}
			return b, nil
		}
	}
	return s.serveReadHandler.LoadBlob(ctx, typ, id, buf)
}

type editReceipt struct {
	Path       string `json:"path,omitempty"`
	ETag       uint64 `json:"etag,omitempty"`
	BeforeETag uint64 `json:"before_etag,omitempty"`
	Deleted    uint64 `json:"deleted,omitempty"`
}

// treeRole is one written tree of a result: the lossless head or its public
// twin. An empty tree has no snapshot.
type treeRole struct {
	Empty            bool    `json:"empty,omitempty"`
	Snapshot         string  `json:"snapshot,omitempty"`
	Tree             string  `json:"tree,omitempty"`
	Entries          uint64  `json:"entries"`
	LogicalBytes     uint64  `json:"logical_bytes"`
	LargestFileBytes *uint64 `json:"largest_file_bytes,omitempty"`
}

type contentReceipt struct {
	IDs []string `json:"ids"`
}

type treeWriteResponse struct {
	Version int      `json:"version"`
	Head    treeRole `json:"head"`
	// Public is absent when the twin is refused (IncompleteLinkGroups).
	Public               *treeRole          `json:"public,omitempty"`
	IncompleteLinkGroups []string           `json:"incomplete_link_groups,omitempty"`
	Edits                []editReceipt      `json:"edits,omitempty"`
	Contents             []contentReceipt   `json:"contents,omitempty"`
	DataAdded            uint64             `json:"data_added"`
	DataAddedPacked      uint64             `json:"data_added_packed"`
	TimingsMS            map[string]float64 `json:"timings_ms,omitempty"`
}

// contentItem is a request content resolved to blob IDs.
type contentItem struct {
	ids    restic.IDs
	size   uint64
	sha256 string
}

// base loads a snapshot, or the empty tree. Snapshots this process wrote or
// loaded are kept until a prune is seen, so a chain of edits reads no snapshot
// file.
func (s *serveWriteHandler) base(ctx context.Context, src treeSource) (*data.Snapshot, restic.ID, error) {
	if src.Empty {
		return nil, restic.ID{}, nil
	}
	id, err := restic.ParseID(src.Snapshot)
	if err != nil {
		return nil, restic.ID{}, invalidf("snapshot %q", src.Snapshot)
	}
	if sn, ok := s.snapshotsSeen[id]; ok {
		return sn, *sn.Tree, nil
	}
	sn, err := data.LoadSnapshot(ctx, s.repo, id)
	if err != nil {
		// serve-read's prepare refreshes the index once and maps a missing
		// snapshot to os.ErrNotExist.
		if _, err = s.prepare(ctx, id); err != nil {
			return nil, restic.ID{}, err
		}
		if sn, err = data.LoadSnapshot(ctx, s.repo, id); err != nil {
			return nil, restic.ID{}, err
		}
	}
	if sn.Tree == nil {
		return nil, restic.ID{}, errors.New("snapshot has no tree")
	}
	s.rememberSnapshot(id, sn)
	return sn, *sn.Tree, nil
}

func (s *serveWriteHandler) rememberSnapshot(id restic.ID, sn *data.Snapshot) {
	if len(s.snapshotsSeen) >= 1024 {
		clear(s.snapshotsSeen)
		clear(s.snapshotIDs)
	}
	s.snapshotsSeen[id] = sn
	s.snapshotIDs[sn] = id
}

func since(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

// prepareWrite computes the statistics of a snapshot's tree, so the first
// write on it does not walk the whole tree. It writes nothing.
func (s *serveWriteHandler) prepareWrite(ctx context.Context, src treeSource) (*treeWriteResponse, error) {
	if err := src.validate("base"); err != nil {
		return nil, err
	}
	start := time.Now()
	resp := &treeWriteResponse{Version: treeWriteVersion, TimingsMS: map[string]float64{}}
	_, root, err := s.base(ctx, src)
	if err != nil {
		return nil, err
	}
	st, err := s.rootStats(ctx, root)
	if err != nil {
		return nil, err
	}
	resp.Head = treeRole{Empty: root.IsNull(), Entries: st.entries, LogicalBytes: st.bytes}
	if !root.IsNull() {
		resp.Head.LargestFileBytes = &st.largest
		resp.Head.Tree = root.String()
		resp.Head.Snapshot = src.Snapshot
	}
	resp.TimingsMS["total"] = since(start)
	return resp, nil
}

// hashContent chunks bytes with the repository's polynomial, as backup does,
// so the same bytes give the same blob IDs. Blobs the index lacks are kept
// until the upload phase.
func (s *serveWriteHandler) hashContent(ctx context.Context, rd io.Reader) (restic.IDs, uint64, error) {
	ch := chunker.New(rd, s.repo.Config().ChunkerPolynomial)
	buf := make([]byte, chunker.MaxSize)
	ids := restic.IDs{}
	var size uint64
	for {
		c, err := ch.Next(buf)
		if err == io.EOF {
			return ids, size, nil
		}
		if err != nil {
			return nil, 0, err
		}
		if err = ctx.Err(); err != nil {
			return nil, 0, err
		}
		id := restic.Hash(c.Data)
		if _, ok := s.repo.LookupBlobSize(restic.DataBlob, id); !ok {
			if _, ok := s.pendingData[id]; !ok {
				s.pendingData[id] = append([]byte(nil), c.Data...)
			}
		}
		ids = append(ids, id)
		size += uint64(c.Length)
	}
}

// build computes the result trees of a request; it saves nothing.
func (s *serveWriteHandler) build(ctx context.Context, req *treeWriteRequest, contents []contentItem, marks map[string]float64) (*data.Snapshot, *finishedTree, []editReceipt, error) {
	start := time.Now()
	baseSn, root, err := s.base(ctx, req.Base)
	if err != nil {
		return nil, nil, nil, err
	}
	marks["load"] = since(start)
	var receipts []editReceipt
	if req.Merge != nil {
		root, err = s.mergeWrite(ctx, req, contents, root, marks)
	} else if !req.PublicTwinOfBase {
		root, receipts, err = s.edits(ctx, req, contents, root)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	f, err := s.check(ctx, root)
	marks["build"] = since(start)
	return baseSn, f, receipts, err
}

func (s *serveWriteHandler) edits(ctx context.Context, req *treeWriteRequest, contents []contentItem, cur restic.ID) (restic.ID, []editReceipt, error) {
	st, err := s.rootStats(ctx, cur)
	if err != nil {
		return restic.ID{}, nil, err
	}
	ino := st.maxInode
	var receipts []editReceipt
	for i := range req.Edits {
		t := s.newEditTree(cur, req.now, *req.Owner, &ino)
		receipt, err := t.apply(ctx, &req.Edits[i], contents)
		var refusal *writeRefusal
		if errors.As(err, &refusal) {
			refusal.Edit = i
		}
		if err != nil {
			return restic.ID{}, nil, err
		}
		if receipt.Path != "" {
			n, err := t.node(ctx, receipt.Path)
			if err != nil {
				return restic.ID{}, nil, err
			}
			receipt.ETag = etagOf(n)
		}
		receipts = append(receipts, receipt)
		if cur, err = t.commit(ctx); err != nil {
			return restic.ID{}, nil, err
		}
	}
	return cur, receipts, nil
}

// write builds, uploads and saves the snapshots of one request. The caller
// holds a shared repository lock.
func (s *serveWriteHandler) write(ctx context.Context, req *treeWriteRequest) (resp *treeWriteResponse, err error) {
	defer func() {
		clear(s.pending)
		clear(s.pendingData)
		if err != nil {
			// The projection cache may map trees to projections that were
			// never saved.
			s.resetProjection()
		}
	}()
	start := time.Now()
	resp = &treeWriteResponse{Version: treeWriteVersion, TimingsMS: map[string]float64{}}
	contents := make([]contentItem, len(req.Contents))
	for i, c := range req.Contents {
		ids, size, err := s.hashContent(ctx, bytes.NewReader(c.Data))
		if err != nil {
			return nil, err
		}
		contents[i] = contentItem{ids: ids, size: size, sha256: c.SHA256}
		resp.Contents = append(resp.Contents, contentReceipt{IDs: idStrings(ids)})
	}
	resp.TimingsMS["content"] = since(start)
	baseSn, f, receipts, err := s.build(ctx, req, contents, resp.TimingsMS)
	if err != nil {
		return nil, err
	}
	resp.Edits = receipts
	if err = s.upload(ctx, f); err != nil {
		return nil, err
	}
	if !f.root.IsNull() {
		s.rememberIndex(ctx)
	}
	resp.TimingsMS["upload"] = since(start)
	if err = s.snapshots(ctx, req, baseSn, f, start, resp); err != nil {
		return nil, err
	}
	resp.TimingsMS["total"] = since(start)
	return resp, nil
}

func idStrings(ids restic.IDs) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

func (s *serveWriteHandler) rootStats(ctx context.Context, root restic.ID) (*treeStats, error) {
	if root.IsNull() {
		return &treeStats{}, nil
	}
	return s.statsOf(ctx, root)
}

type finishedTree struct {
	root, public restic.ID
	stats        *treeStats
	incomplete   []string
}

// check validates a new root before anything is saved. The head must hold
// every name of each hard-linked inode; the twin is not written when a group
// has a private name and a public one (decision 28), as the helper refuses an
// incomplete group in its public scan.
func (s *serveWriteHandler) check(ctx context.Context, root restic.ID) (*finishedTree, error) {
	out := &finishedTree{root: root, stats: &treeStats{}}
	if root.IsNull() {
		return out, nil
	}
	st, err := s.statsOf(ctx, root)
	if err != nil {
		return nil, err
	}
	out.stats = st
	for _, names := range st.linkGroups() {
		public := []string{}
		for _, l := range names {
			if !l.private {
				public = append(public, "/"+l.rel)
			}
		}
		if uint64(len(names)) != names[0].links {
			return nil, errors.New("incomplete hard-link group in the head")
		}
		if len(public) > 0 && len(public) != len(names) {
			sort.Strings(public)
			out.incomplete = append(out.incomplete, public[0])
		}
	}
	sort.Strings(out.incomplete)
	if len(out.incomplete) == 0 && st.pubEntries > 0 {
		out.public, err = s.public.RewriteTree(ctx, s, pendingSaver{s}, "/", root)
	}
	return out, err
}

// pendingSaver keeps new trees of the public projection pending, so the
// projection runs before anything is saved.
type pendingSaver struct{ s *serveWriteHandler }

func (p pendingSaver) SaveBlob(_ context.Context, t restic.BlobType, buf []byte, id restic.ID, _ bool) (restic.ID, bool, int, error) {
	if t != restic.TreeBlob {
		return restic.ID{}, false, 0, errors.New("projection saves only trees")
	}
	if id.IsNull() {
		id = restic.Hash(buf)
	}
	if _, ok := p.s.pending[id]; ok {
		return id, true, 0, nil
	}
	if _, ok := p.s.repo.LookupBlobSize(restic.TreeBlob, id); ok && !p.s.verifier {
		return id, true, 0, nil
	}
	p.s.pending[id] = append([]byte(nil), buf...)
	return id, false, len(buf), nil
}

// upload saves the pending blobs the root and the public twin reference in
// one pack upload, then writes the index. Any failure is an uploadError: the
// repository handle cannot upload again.
func (s *serveWriteHandler) upload(ctx context.Context, f *finishedTree) error {
	s.added = addedStats{}
	if f.root.IsNull() {
		return nil
	}
	saved := map[restic.ID][]byte{}
	err := s.repo.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) error {
		if err := s.savePending(ctx, up, f.root, saved); err != nil {
			return err
		}
		return s.savePending(ctx, up, f.public, saved)
	})
	if err != nil {
		return &uploadError{err}
	}
	// Only trees that reached the index enter the blob cache.
	for id, buf := range saved {
		s.cache.Add(id, buf)
	}
	return nil
}

// savePending saves the pending trees reachable from id and the new data
// blobs their files reference; trees of intermediate edits that the final
// root does not reference are dropped.
func (s *serveWriteHandler) savePending(ctx context.Context, up restic.BlobSaver, id restic.ID, saved map[restic.ID][]byte) error {
	buf, ok := s.pending[id]
	if !ok {
		return nil
	}
	nodes, err := s.loadNodes(ctx, id, false)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		switch n.Type {
		case data.NodeTypeDir:
			if err = s.savePending(ctx, up, *n.Subtree, saved); err != nil {
				return err
			}
		case data.NodeTypeFile:
			for _, c := range n.Content {
				if b, ok := s.pendingData[c]; ok {
					_, known, size, err := up.SaveBlob(ctx, restic.DataBlob, b, c, false)
					if err != nil {
						return err
					}
					if !known {
						s.added.dataAdded += uint64(len(b))
						s.added.dataAddedPacked += uint64(size)
						s.added.dataBlobs++
					}
					delete(s.pendingData, c)
				}
			}
		}
	}
	_, known, size, err := up.SaveBlob(ctx, restic.TreeBlob, buf, id, false)
	if err != nil {
		return err
	}
	if !known {
		s.added.dataAdded += uint64(len(buf))
		s.added.dataAddedPacked += uint64(size)
		s.added.treeBlobs++
	}
	delete(s.pending, id)
	saved[id] = buf
	return nil
}

// snapshotOf is the snapshot a request writes for tree, before tags and
// statistics.
func snapshotOf(req *treeWriteRequest, baseSn *data.Snapshot, baseID restic.ID, tree restic.ID) *data.Snapshot {
	sn := &data.Snapshot{Paths: req.Paths, Hostname: req.Hostname, Username: "root", ProgramVersion: "restic " + global.Version}
	if baseSn != nil {
		sn.Paths, sn.Username, sn.UID, sn.GID = baseSn.Paths, baseSn.Username, baseSn.UID, baseSn.GID
		sn.Parent = &baseID
	}
	sn.Tags = append([]string(nil), req.Tags...)
	sn.Tree = &tree
	return sn
}

// snapshots writes the head and then its public twin. The pack and index
// uploads have completed. The head's summary carries the request's added
// data; the twin's carries none, so the two are not counted twice.
func (s *serveWriteHandler) snapshots(ctx context.Context, req *treeWriteRequest, baseSn *data.Snapshot, f *finishedTree, start time.Time, resp *treeWriteResponse) error {
	resp.DataAdded, resp.DataAddedPacked = s.added.dataAdded, s.added.dataAddedPacked
	if f.root.IsNull() {
		resp.Head = treeRole{Empty: true}
		resp.Public = &treeRole{Empty: true}
		return nil
	}
	baseID, _ := restic.ParseID(req.Base.Snapshot)
	sn := snapshotOf(req, baseSn, baseID, f.root)
	if req.PublicTwinOfBase {
		sn.Tags = append([]string(nil), baseSn.Tags...)
	}
	sn.Time = time.Now()
	sn.Summary = &data.SnapshotSummary{BackupStart: start, BackupEnd: sn.Time, DataBlobs: s.added.dataBlobs, TreeBlobs: s.added.treeBlobs,
		DataAdded: s.added.dataAdded, DataAddedPacked: s.added.dataAddedPacked,
		TotalFilesProcessed: uint(f.stats.files), TotalBytesProcessed: f.stats.bytes}
	id := baseID
	if !req.PublicTwinOfBase {
		var err error
		id, err = data.SaveSnapshot(ctx, s.repo, sn)
		if err != nil {
			return err
		}
		s.rememberSnapshot(id, sn)
	}
	resp.Head = treeRole{Snapshot: id.String(), Tree: f.root.String(), Entries: f.stats.entries, LogicalBytes: f.stats.bytes, LargestFileBytes: &f.stats.largest}
	resp.TimingsMS["head_snapshot"] = since(start)
	resp.IncompleteLinkGroups = f.incomplete
	switch {
	case len(f.incomplete) > 0:
	case f.stats.pubEntries == 0:
		resp.Public = &treeRole{Empty: true}
	default:
		pub := *sn
		pub.Tags = append(append([]string(nil), sn.Tags...), req.PublicTags...)
		pub.Parent = &id
		pubTree := f.public
		pub.Tree = &pubTree
		pub.Summary = &data.SnapshotSummary{BackupStart: start, BackupEnd: sn.Time, TotalFilesProcessed: uint(f.stats.pubFiles), TotalBytesProcessed: f.stats.pubBytes}
		if req.PublicTwinOfBase {
			pub.Summary.DataAdded, pub.Summary.DataAddedPacked = resp.DataAdded, resp.DataAddedPacked
			pub.Summary.DataBlobs, pub.Summary.TreeBlobs = s.added.dataBlobs, s.added.treeBlobs
		}
		pubID, err := data.SaveSnapshot(ctx, s.repo, &pub)
		if err != nil {
			return err
		}
		s.rememberSnapshot(pubID, &pub)
		resp.Public = &treeRole{Snapshot: pubID.String(), Tree: f.public.String(), Entries: f.stats.pubEntries, LogicalBytes: f.stats.pubBytes, LargestFileBytes: &f.stats.pubLargest}
	}
	return nil
}
