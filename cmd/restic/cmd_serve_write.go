package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/walker"
	"github.com/spf13/cobra"
)

const (
	serveWriteBodyLimit  = 96 << 20
	serveWriteStatsLimit = 1 << 21
	defaultOwner         = 65532
)

// defaultPublicExcludes is what a public twin leaves out at every depth: the
// helper's publicExcludes (storagewire.WorkspacePrivateNames plus lost+found
// and NFS silly-rename names), matched case-insensitively per name.
func defaultPublicExcludes() []string {
	return []string{"lost+found", ".nfs*", ".forge", ".trash", ".plori-trash", ".plori-workspace",
		".control", ".config", ".jfs", ".stats", ".accesslog"}
}

func newServeWriteCommand(gopts *global.Options) *cobra.Command {
	var socket string
	var excludes []string
	cmd := &cobra.Command{Use: "serve-write --socket PATH", Short: "Serve snapshot reads and tree-native snapshot writes over a private Unix socket", GroupID: cmdGroupAdvanced, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if socket == "" {
				return errors.New("--socket is required")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			printer := ui.NewProgressPrinter(false, gopts.Verbosity, gopts.Term)
			ctx, repo, unlock, err := openWithAppendLock(ctx, *gopts, false, printer)
			if err != nil {
				return err
			}
			defer unlock()
			if err = repo.LoadIndex(ctx, printer); err != nil {
				return err
			}
			if err := serveReadListen(ctx, socket, newServeWriteHandler(repo, excludes)); err != nil {
				return err
			}
			return ErrOK
		}}
	cmd.Flags().StringVar(&socket, "socket", "", "platform Unix socket `path` (mode 0600)")
	cmd.Flags().StringArrayVar(&excludes, "public-exclude", defaultPublicExcludes(), "name `pattern` the public twin leaves out at every depth")
	return cmd
}

type serveWriteHandler struct {
	*serveReadHandler
	excludes []string
	stats    map[restic.ID]*treeStats
	statsMu  sync.Mutex
	walkers  chan struct{}
	public   *walker.TreeRewriter
	// pending and pendingData hold the tree and data blobs of the current
	// request. A request computes its whole result before it saves anything,
	// so a refusal or a malformed edit leaves the repository untouched.
	pending     map[restic.ID][]byte
	pendingData map[restic.ID][]byte
	tokens      map[restic.ID]map[string]restic.IDs
	// broken is set when an upload failed: upstream's WithBlobUploader does
	// not reset the repository after an error, so the process must restart.
	broken bool
	// snapshotsSeen and snapshotIDs keep loaded and written snapshots.
	snapshotsSeen map[restic.ID]*data.Snapshot
	snapshotIDs   map[*data.Snapshot]restic.ID
}

func newServeWriteHandler(repo *repository.Repository, excludes []string) *serveWriteHandler {
	s := &serveWriteHandler{serveReadHandler: newServeReadHandler(repo), excludes: excludes,
		stats: map[restic.ID]*treeStats{}, walkers: make(chan struct{}, 8), pending: map[restic.ID][]byte{}, pendingData: map[restic.ID][]byte{}, tokens: map[restic.ID]map[string]restic.IDs{},
		snapshotsSeen: map[restic.ID]*data.Snapshot{}, snapshotIDs: map[*data.Snapshot]restic.ID{}}
	s.resetProjection()
	return s
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

// LoadBlob serves trees this request encoded but has not saved yet.
func (s *serveWriteHandler) LoadBlob(ctx context.Context, typ restic.BlobType, id restic.ID, buf []byte) ([]byte, error) {
	if typ == restic.TreeBlob {
		if b, ok := s.pending[id]; ok {
			return b, nil
		}
	}
	return s.serveReadHandler.LoadBlob(ctx, typ, id, buf)
}

type writeEdit struct {
	Op           string  `json:"op"`
	Path         string  `json:"path"`
	To           string  `json:"to,omitempty"`
	Trash        string  `json:"trash,omitempty"`
	Data         []byte  `json:"data,omitempty"`
	Source       string  `json:"source,omitempty"`
	Mode         uint32  `json:"mode,omitempty"`
	ExpectedETag *uint64 `json:"expected_etag,omitempty"`
}

type mergeEntry struct {
	Path      string `json:"path"`
	Kind      string `json:"kind"`
	Mode      uint32 `json:"mode"`
	Size      int64  `json:"size"`
	Digest    string `json:"digest,omitempty"`
	Target    string `json:"target,omitempty"`
	LinkGroup string `json:"link_group,omitempty"`
}

type writeRequest struct {
	Snapshot string            `json:"snapshot,omitempty"`
	Base     string            `json:"base,omitempty"`
	OpID     string            `json:"op_id,omitempty"`
	CopyID   string            `json:"copy_id,omitempty"`
	Paths    []string          `json:"paths,omitempty"`
	Owner    *[2]uint32        `json:"owner,omitempty"`
	Edits    []writeEdit       `json:"edits,omitempty"`
	Entries  []mergeEntry      `json:"entries,omitempty"`
	Sources  []string          `json:"sources,omitempty"`
	Blobs    map[string][]byte `json:"blobs,omitempty"`
}

type snapshotReceipt struct {
	Snapshot     string `json:"snapshot"`
	Tree         string `json:"tree"`
	Entries      uint64 `json:"entries"`
	LogicalBytes uint64 `json:"logical_bytes"`
}

type editReceipt struct {
	Path string `json:"path,omitempty"`
	ETag uint64 `json:"etag,omitempty"`
}

type writeResponse struct {
	Empty                bool               `json:"empty"`
	Head                 *snapshotReceipt   `json:"head,omitempty"`
	Public               *snapshotReceipt   `json:"public,omitempty"`
	PublicEmpty          bool               `json:"public_empty"`
	IncompleteLinkGroups []string           `json:"incomplete_link_groups,omitempty"`
	Edits                []editReceipt      `json:"edits,omitempty"`
	TimingsMS            map[string]float64 `json:"timings_ms"`
}

func (s *serveWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/prepare-write", "/edit", "/merge-write":
	default:
		s.serveReadHandler.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		serveReadError(w, 405)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, serveWriteBodyLimit)
	var req writeRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if dec.Decode(&req) != nil {
		serveReadError(w, 400)
		return
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		serveReadError(w, 400)
		return
	}
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-r.Context().Done():
		return
	}
	if s.broken {
		serveReadError(w, http.StatusServiceUnavailable)
		return
	}
	var resp *writeResponse
	var err error
	switch r.URL.Path {
	case "/prepare-write":
		resp, err = s.prepareWrite(r.Context(), &req)
	case "/edit":
		resp, err = s.edit(r.Context(), &req)
	case "/merge-write":
		resp, err = s.mergeWrite(r.Context(), &req)
	}
	clear(s.pending)
	clear(s.pendingData)
	var refusal *writeRefusal
	switch {
	case errors.As(err, &refusal):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(refusal)
		return
	case errors.Is(err, errInvalid):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	case errors.Is(err, os.ErrNotExist):
		serveReadError(w, 404)
		return
	case err != nil:
		// The projection cache may map trees to projections that were
		// never saved.
		s.resetProjection()
		http.Error(w, "write failed", http.StatusInternalServerError)
		_, _ = fmt.Fprintf(os.Stderr, "serve-write: %v\n", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// base loads a full snapshot ID, or the empty tree for "". Snapshots this
// process wrote or loaded are kept, so a chain of edits reads no snapshot
// file and never refreshes the index.
func (s *serveWriteHandler) base(ctx context.Context, selector string) (*data.Snapshot, restic.ID, error) {
	if selector == "" {
		return nil, restic.ID{}, nil
	}
	id, err := restic.ParseID(selector)
	if err != nil {
		return nil, restic.ID{}, invalidf("snapshot %q", selector)
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

func (r *writeRequest) owner() [2]uint32 {
	if r.Owner != nil {
		return *r.Owner
	}
	return [2]uint32{defaultOwner, defaultOwner}
}

func since(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

// prepareWrite computes the statistics and the public projection of a
// snapshot, so the first edit on it does not walk the whole tree.
func (s *serveWriteHandler) prepareWrite(ctx context.Context, req *writeRequest) (*writeResponse, error) {
	start := time.Now()
	resp := &writeResponse{TimingsMS: map[string]float64{}}
	_, root, err := s.base(ctx, req.Snapshot)
	if err != nil || root.IsNull() {
		return resp, err
	}
	st, err := s.statsOf(ctx, root)
	if err != nil {
		return nil, err
	}
	resp.TimingsMS["stats"] = since(start)
	if st.pubEntries > 0 {
		f := &finishedTree{root: root, stats: st}
		if f.public, err = s.public.RewriteTree(ctx, s, pendingSaver{s}, "/", root); err == nil {
			err = s.upload(ctx, f)
		}
	}
	resp.TimingsMS["total"] = since(start)
	return resp, err
}

type contentSaver func(context.Context, *writeEdit) (restic.IDs, uint64, error)

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

func (s *serveWriteHandler) editContent(ctx context.Context, e *writeEdit) (restic.IDs, uint64, error) {
	if e.Source == "" {
		return s.hashContent(ctx, bytes.NewReader(e.Data))
	}
	if len(e.Data) != 0 {
		return nil, 0, invalidf("data and source")
	}
	f, err := os.Open(e.Source)
	if err != nil {
		return nil, 0, invalidf("source unreadable")
	}
	defer func() { _ = f.Close() }()
	return s.hashContent(ctx, f)
}

func (s *serveWriteHandler) edit(ctx context.Context, req *writeRequest) (*writeResponse, error) {
	start := time.Now()
	if len(req.Edits) == 0 || req.OpID == "" {
		return nil, invalidf("edits and op_id are required")
	}
	baseSn, cur, err := s.base(ctx, req.Base)
	if err != nil {
		return nil, err
	}
	resp := &writeResponse{TimingsMS: map[string]float64{"load": since(start)}}
	st, err := s.rootStats(ctx, cur)
	if err != nil {
		return nil, err
	}
	ino := st.maxInode
	now := time.Now()
	for i := range req.Edits {
		t := s.newEditTree(cur, now, req.owner(), &ino)
		target, err := t.apply(ctx, &req.Edits[i], s.editContent)
		var refusal *writeRefusal
		if errors.As(err, &refusal) {
			refusal.Edit = i
		}
		if err != nil {
			return nil, err
		}
		receipt := editReceipt{Path: target}
		if target != "" {
			n, err := t.node(ctx, target)
			if err != nil {
				return nil, err
			}
			receipt.ETag = etagOf(n)
		}
		resp.Edits = append(resp.Edits, receipt)
		if cur, err = t.commit(ctx); err != nil {
			return nil, err
		}
	}
	f, err := s.check(ctx, cur)
	if err != nil {
		return nil, err
	}
	resp.TimingsMS["edit"] = since(start)
	if err = s.upload(ctx, f); err != nil {
		return nil, err
	}
	resp.TimingsMS["flush"] = since(start)
	return s.snapshots(ctx, req, baseSn, f, start, resp)
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
	out := &finishedTree{root: root}
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
	if _, ok := p.s.repo.LookupBlobSize(restic.TreeBlob, id); ok {
		return id, true, 0, nil
	}
	if _, ok := p.s.pending[id]; ok {
		return id, true, 0, nil
	}
	p.s.pending[id] = append([]byte(nil), buf...)
	return id, false, len(buf), nil
}

// upload saves the pending blobs the root and the public twin reference in
// one pack upload, then writes the index. A failure leaves the
// repository object unusable for writes (see broken).
func (s *serveWriteHandler) upload(ctx context.Context, f *finishedTree) error {
	if f.root.IsNull() {
		return nil
	}
	err := s.repo.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) error {
		if err := s.savePending(ctx, up, f.root); err != nil {
			return err
		}
		return s.savePending(ctx, up, f.public)
	})
	if err != nil {
		s.broken = true
	}
	return err
}

// savePending saves the pending trees reachable from id and the new data
// blobs their files reference; trees of intermediate edits that the final
// root does not reference are dropped.
func (s *serveWriteHandler) savePending(ctx context.Context, up restic.BlobSaver, id restic.ID) error {
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
			if err = s.savePending(ctx, up, *n.Subtree); err != nil {
				return err
			}
		case data.NodeTypeFile:
			for _, c := range n.Content {
				if b, ok := s.pendingData[c]; ok {
					if _, _, _, err = up.SaveBlob(ctx, restic.DataBlob, b, c, false); err != nil {
						return err
					}
					delete(s.pendingData, c)
				}
			}
		}
	}
	if _, _, _, err = up.SaveBlob(ctx, restic.TreeBlob, buf, id, false); err != nil {
		return err
	}
	delete(s.pending, id)
	s.cache.Add(id, buf)
	return nil
}

// snapshots writes the head and then its public twin with the helper's tags
// and host. The pack and index uploads have completed.
func (s *serveWriteHandler) snapshots(ctx context.Context, req *writeRequest, baseSn *data.Snapshot, f *finishedTree, start time.Time, resp *writeResponse) (*writeResponse, error) {
	if f.root.IsNull() {
		resp.Empty, resp.PublicEmpty = true, true
		resp.TimingsMS["total"] = since(start)
		return resp, nil
	}
	sn := &data.Snapshot{Paths: req.Paths, Hostname: "plori-workspace", Username: "root", ProgramVersion: "restic " + global.Version}
	if baseSn != nil {
		sn.Paths, sn.Username, sn.UID, sn.GID = baseSn.Paths, baseSn.Username, baseSn.UID, baseSn.GID
		parent := s.snapshotIDs[baseSn]
		sn.Parent = &parent
	}
	if len(sn.Paths) == 0 {
		return nil, invalidf("paths are required without a base snapshot")
	}
	sn.Time = time.Now()
	sn.Tags = []string{"plori-op:" + req.OpID}
	if req.CopyID != "" {
		sn.Tags = append(sn.Tags, "plori-copy:"+req.CopyID)
	}
	tree := f.root
	sn.Tree = &tree
	sn.Summary = &data.SnapshotSummary{BackupStart: start, BackupEnd: sn.Time, TotalFilesProcessed: uint(f.stats.files), TotalBytesProcessed: f.stats.bytes}
	id, err := data.SaveSnapshot(ctx, s.repo, sn)
	if err != nil {
		return nil, err
	}
	s.rememberSnapshot(id, sn)
	resp.Head = &snapshotReceipt{id.String(), f.root.String(), f.stats.entries, f.stats.bytes}
	resp.TimingsMS["head_snapshot"] = since(start)
	resp.IncompleteLinkGroups = f.incomplete
	switch {
	case len(f.incomplete) > 0:
	case f.stats.pubEntries == 0:
		resp.PublicEmpty = true
	default:
		pub := *sn
		pub.Tags = append(append([]string(nil), sn.Tags...), "plori-public")
		pub.Parent = &id
		pubTree := f.public
		pub.Tree = &pubTree
		pub.Summary = &data.SnapshotSummary{BackupStart: start, BackupEnd: sn.Time, TotalFilesProcessed: uint(f.stats.pubFiles), TotalBytesProcessed: f.stats.pubBytes}
		pubID, err := data.SaveSnapshot(ctx, s.repo, &pub)
		if err != nil {
			return nil, err
		}
		s.rememberSnapshot(pubID, &pub)
		resp.Public = &snapshotReceipt{pubID.String(), f.public.String(), f.stats.pubEntries, f.stats.pubBytes}
	}
	resp.TimingsMS["total"] = since(start)
	return resp, nil
}
