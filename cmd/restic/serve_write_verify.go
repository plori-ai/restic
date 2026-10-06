package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"slices"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

// verifyFailure is a definite mismatch between a write result and the
// repository; verify-write answers it with ok=false.
type verifyFailure struct {
	Code   string
	Detail string
}

func (f *verifyFailure) Error() string { return "verify: " + f.Code + ": " + f.Detail }

func mismatch(code, format string, args ...any) error {
	return &verifyFailure{Code: code, Detail: fmt.Sprintf(format, args...)}
}

type verifyWriteResponse struct {
	Version               int                `json:"version"`
	OK                    bool               `json:"ok"`
	Code                  string             `json:"code,omitempty"`
	Detail                string             `json:"detail,omitempty"`
	TimingsMS             map[string]float64 `json:"timings_ms"`
	HeadManifestValidated bool               `json:"head_manifest_validated"`
}

// indexed requires that the blobs a node references are in the verifier's
// index: every content blob, with sizes that sum to the file size, and the
// subtree unless this replay encoded it.
func (s *serveWriteHandler) indexed(n *data.Node) error {
	switch n.Type {
	case data.NodeTypeFile:
		var total uint64
		for _, id := range n.Content {
			size, ok := s.repo.LookupBlobSize(restic.DataBlob, id)
			if !ok {
				return mismatch("blob_missing", "data blob %v of %q", id.Str(), n.Name)
			}
			total += uint64(size)
		}
		if total != n.Size {
			return mismatch("count_mismatch", "content of %q has %d bytes, size %d", n.Name, total, n.Size)
		}
	case data.NodeTypeDir:
		if n.Subtree == nil {
			return mismatch("tree_unreadable", "directory %q has no subtree", n.Name)
		}
		if _, ok := s.pending[*n.Subtree]; ok {
			return nil
		}
		if _, ok := s.repo.LookupBlobSize(restic.TreeBlob, *n.Subtree); !ok {
			return mismatch("blob_missing", "tree %v of %q", n.Subtree.Str(), n.Name)
		}
	}
	return nil
}

// verifyWrite checks a tree-write result against the repository through the
// verifier's own handle, whose index holds exactly the index files the
// backend lists now (refreshIndex). It replays the request on its source to
// check the plan, then loads the candidate's new trees from the backend,
// bypassing every cache, and walks both trees, requiring every referenced
// blob to be in that index. Nothing the writer holds in memory or in its local
// cache is used.
//
// The walk is incremental: the statistics of a tree are kept across calls
// only when this handle computed them from trees it loaded itself, with every
// referenced blob in its index. Trees are immutable and index files are only
// added between prunes, so such a tree needs no new walk until an index file
// disappears, which drops all statistics (refreshIndex). The candidate's new
// trees are never taken from earlier calls.
func (s *serveWriteHandler) verifyWrite(ctx context.Context, v *verifyWriteRequest) (*verifyWriteResponse, error) {
	start := time.Now()
	resp := &verifyWriteResponse{Version: treeWriteVersion, TimingsMS: map[string]float64{}}
	err := s.verify(ctx, &v.Request, &v.Result, resp.TimingsMS)
	s.resetCall()
	var failure *verifyFailure
	switch {
	case errors.As(err, &failure):
		resp.Code, resp.Detail = failure.Code, failure.Detail
	case errors.Is(err, os.ErrNotExist):
		resp.Code = "snapshot_missing"
	case err != nil:
		if ctx.Err() == nil {
			_, _ = fmt.Fprintf(os.Stderr, "serve-write: verify: %v\n", err)
		}
		return nil, &writeFailureError{status: http.StatusServiceUnavailable, code: "verify_unavailable"}
	default:
		resp.OK = true
		resp.HeadManifestValidated = s.workspaceHeadManifest(v.Result.Head)
	}
	resp.TimingsMS["total"] = since(start)
	return resp, nil
}

// workspaceHeadManifest certifies the complete portable-reader contract from
// this independent verifier's own authenticated tree statistics. The writer's
// memory and caches are never a source for this receipt.
func (s *serveWriteHandler) workspaceHeadManifest(head treeRole) bool {
	if head.Empty {
		return true
	}
	root, err := restic.ParseID(head.Tree)
	if err != nil {
		return false
	}
	st, ok := s.cachedStats(root)
	if !ok || !st.workspaceManifest {
		return false
	}
	for _, group := range st.linkGroups() {
		for _, name := range group {
			if name.links != uint64(len(group)) {
				return false
			}
		}
	}
	return true
}

func (s *serveWriteHandler) verify(ctx context.Context, req *treeWriteRequest, res *treeWriteResponse, marks map[string]float64) error {
	start := time.Now()
	s.resetCall()
	// Statistics of trees this replay encoded were computed from memory, not
	// from the backend: they never outlive the call.
	defer func() {
		s.statsMu.Lock()
		for id := range s.pending {
			delete(s.stats, id)
		}
		s.statsMu.Unlock()
	}()
	if err := s.refreshIndex(ctx, marks); err != nil {
		return err
	}
	contents, err := s.verifyContents(ctx, req, res)
	if err != nil {
		return err
	}
	marks["content"] = since(start)
	baseSn, f, receipts, err := s.build(ctx, req, contents, marks)
	var refusal *writeRefusal
	if errors.As(err, &refusal) {
		return mismatch("plan_mismatch", "replay refused edit %d: %s", refusal.Edit, refusal.Code)
	}
	if errors.Is(err, os.ErrNotExist) {
		return mismatch("source_missing", "a source snapshot does not exist")
	}
	if err != nil {
		return err
	}
	marks["replay"] = since(start)
	if !reflect.DeepEqual(receipts, res.Edits) {
		return mismatch("plan_mismatch", "edit receipts differ")
	}
	if err = comparePlan("head", f.root, &res.Head); err != nil {
		return err
	}
	if !slices.Equal(f.incomplete, res.IncompleteLinkGroups) {
		return mismatch("plan_mismatch", "incomplete link groups %v, result %v", f.incomplete, res.IncompleteLinkGroups)
	}
	switch {
	case len(f.incomplete) > 0 || f.root.IsNull():
		if (res.Public == nil) != (len(f.incomplete) > 0) || (res.Public != nil && !res.Public.Empty) {
			return mismatch("plan_mismatch", "public role")
		}
	case f.stats.pubEntries == 0:
		if res.Public == nil || !res.Public.Empty {
			return mismatch("plan_mismatch", "public twin should be empty")
		}
	default:
		if res.Public == nil {
			return mismatch("plan_mismatch", "public twin missing")
		}
		if err = comparePlan("public", f.public, res.Public); err != nil {
			return err
		}
	}
	// Re-read: drop what the replay encoded and load those trees from the
	// backend. A tree the writer did not upload, or whose pack or index
	// entry is gone, fails here.
	s.statsMu.Lock()
	for id := range s.pending {
		delete(s.stats, id)
		s.bypass.Insert(id)
	}
	s.statsMu.Unlock()
	clear(s.pending)
	if f.root.IsNull() {
		marks["reread"] = since(start)
		return nil
	}
	if err = s.verifyRole(ctx, "head", f.root, &res.Head, false); err != nil {
		return err
	}
	if res.Public != nil && !res.Public.Empty {
		if err = s.verifyRole(ctx, "public", f.public, res.Public, true); err != nil {
			return err
		}
	}
	marks["reread"] = since(start)
	if err = s.verifySnapshots(ctx, req, res, baseSn); err != nil {
		return err
	}
	marks["snapshots"] = since(start)
	return nil
}

func comparePlan(role string, got restic.ID, want *treeRole) error {
	if got.IsNull() != want.Empty || (!got.IsNull() && got.String() != want.Tree) {
		return mismatch("plan_mismatch", "%s tree %v, result %q (empty %v)", role, got.Str(), want.Tree, want.Empty)
	}
	return nil
}

// verifyContents resolves the result's blob IDs of each request content: every
// blob must be in the index, and the bytes read from the backend must have the
// declared length and SHA-256.
func (s *serveWriteHandler) verifyContents(ctx context.Context, req *treeWriteRequest, res *treeWriteResponse) ([]contentItem, error) {
	out := make([]contentItem, len(req.Contents))
	for i, c := range req.Contents {
		h := sha256.New()
		var size uint64
		ids := restic.IDs{}
		for _, raw := range res.Contents[i].IDs {
			id, _ := restic.ParseID(raw)
			if _, ok := s.repo.LookupBlobSize(restic.DataBlob, id); !ok {
				return nil, mismatch("blob_missing", "data blob %v of content %d", id.Str(), i)
			}
			buf, err := s.repo.LoadBlob(ctx, restic.DataBlob, id, nil)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, mismatch("content_mismatch", "data blob %v of content %d is unreadable", id.Str(), i)
			}
			_, _ = h.Write(buf)
			size += uint64(len(buf))
			ids = append(ids, id)
		}
		if size != uint64(c.Length) || hex.EncodeToString(h.Sum(nil)) != c.SHA256 {
			return nil, mismatch("content_mismatch", "content %d", i)
		}
		out[i] = contentItem{ids: ids, size: size, sha256: c.SHA256}
	}
	return out, nil
}

// verifyRole walks one written tree from the backend and compares its counts.
func (s *serveWriteHandler) verifyRole(ctx context.Context, role string, root restic.ID, want *treeRole, public bool) error {
	if _, ok := s.repo.LookupBlobSize(restic.TreeBlob, root); !ok {
		return mismatch("blob_missing", "%s root tree %v", role, root.Str())
	}
	st, err := s.statsOf(ctx, root)
	if err != nil {
		return err
	}
	entries, bytes := st.entries, st.bytes
	if public && st.pubEntries != st.entries {
		return mismatch("projection_mismatch", "public tree has %d private entries", st.entries-st.pubEntries)
	}
	if entries != want.Entries || bytes != want.LogicalBytes {
		return mismatch("count_mismatch", "%s has %d entries and %d bytes, result %d and %d", role, entries, bytes, want.Entries, want.LogicalBytes)
	}
	if want.LargestFileBytes == nil {
		return mismatch("count_mismatch", "%s result has no largest file size", role)
	}
	if *want.LargestFileBytes != st.largest {
		return mismatch("count_mismatch", "%s largest file is %d bytes, result %d", role, st.largest, *want.LargestFileBytes)
	}
	return nil
}

// verifySnapshots loads both snapshots by ID from the backend and compares
// them with what the request writes.
func (s *serveWriteHandler) verifySnapshots(ctx context.Context, req *treeWriteRequest, res *treeWriteResponse, baseSn *data.Snapshot) error {
	baseID, _ := restic.ParseID(req.Base.Snapshot)
	want := snapshotOf(req, baseSn, baseID, restic.ID{})
	headID, _ := restic.ParseID(res.Head.Snapshot)
	if req.PublicTwinOfBase {
		want.Tags = append([]string(nil), baseSn.Tags...)
		if headID != baseID {
			return mismatch("snapshot_mismatch", "public twin changed the base head")
		}
		if res.Public == nil || res.Public.Empty {
			if res.DataAdded != 0 || res.DataAddedPacked != 0 {
				return mismatch("snapshot_mismatch", "no public snapshot but added data")
			}
			return nil
		}
		pubID, _ := restic.ParseID(res.Public.Snapshot)
		pub, err := s.loadSnapshot(ctx, "public", pubID)
		if err != nil {
			return err
		}
		want.Tags = append(want.Tags, req.PublicTags...)
		want.Parent = &baseID
		if err := sameSnapshot("public", pub, want, res.Public.Tree); err != nil {
			return err
		}
		if pub.Summary == nil || pub.Summary.DataAdded != res.DataAdded || pub.Summary.DataAddedPacked != res.DataAddedPacked {
			return mismatch("snapshot_mismatch", "public summary does not record added data")
		}
		return nil
	}
	head, err := s.loadSnapshot(ctx, "head", headID)
	if err != nil {
		return err
	}
	if err = sameSnapshot("head", head, want, res.Head.Tree); err != nil {
		return err
	}
	if head.Summary == nil || head.Summary.DataAdded != res.DataAdded || head.Summary.DataAddedPacked != res.DataAddedPacked {
		return mismatch("snapshot_mismatch", "head summary does not record the result's added data")
	}
	if res.Public == nil || res.Public.Empty {
		return nil
	}
	pubID, _ := restic.ParseID(res.Public.Snapshot)
	pub, err := s.loadSnapshot(ctx, "public", pubID)
	if err != nil {
		return err
	}
	want.Tags = append(want.Tags, req.PublicTags...)
	want.Parent = &headID
	return sameSnapshot("public", pub, want, res.Public.Tree)
}

// loadSnapshot reads a snapshot from the backend (the verifier has no
// cache). Only a failed read lists the snapshots, to tell a missing snapshot
// from a backend failure.
func (s *serveWriteHandler) loadSnapshot(ctx context.Context, role string, id restic.ID) (*data.Snapshot, error) {
	sn, err := data.LoadSnapshot(ctx, s.repo, id)
	if err == nil {
		return sn, nil
	}
	found := false
	listErr := s.repo.List(ctx, restic.SnapshotFile, func(c restic.ID, _ int64) error {
		found = found || c == id
		return nil
	})
	if listErr == nil && !found {
		return nil, mismatch("snapshot_missing", "%s snapshot %v", role, id.Str())
	}
	return nil, err
}

func sameSnapshot(role string, got, want *data.Snapshot, tree string) error {
	switch {
	case got.Tree == nil || got.Tree.String() != tree:
		return mismatch("snapshot_mismatch", "%s snapshot tree", role)
	case !reflect.DeepEqual(got.Parent, want.Parent):
		return mismatch("snapshot_mismatch", "%s snapshot parent", role)
	case got.Hostname != want.Hostname || !slices.Equal(got.Paths, want.Paths) || !slices.Equal(got.Tags, want.Tags):
		return mismatch("snapshot_mismatch", "%s snapshot host, paths or tags", role)
	}
	return nil
}
