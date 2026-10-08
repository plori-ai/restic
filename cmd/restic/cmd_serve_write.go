package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

// treeWriteVersion is the socket protocol version of doc/plori-tree-write.md.
// A request with another version is refused.
const treeWriteVersion = 1

type serveWriteConfig struct {
	// excludes are the name patterns a public twin leaves out at every depth.
	excludes []string
	// trashDir is the root directory name the trash, restore and empty-trash
	// edits use; those edits are refused when it is empty.
	trashDir string
	// maxRequest bounds a tree-write or verify-write request body.
	maxRequest int64
}

// repoOpener opens the repository. The verifier handle must not use the local
// cache: restic's cache stores the tree packs this process writes.
type repoOpener func(ctx context.Context, verifier bool) (*repository.Repository, error)

func newServeWriteCommand(gopts *global.Options) *cobra.Command {
	var socket string
	cfg := serveWriteConfig{}
	reads := defaultContentOptions()
	cmd := &cobra.Command{Use: "serve-write --socket PATH", Short: "Serve snapshot reads and tree-native snapshot writes (including public-twin-of-base) over a private Unix socket", GroupID: cmdGroupAdvanced, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if socket == "" {
				return errors.New("--socket is required")
			}
			if cfg.maxRequest < 1<<20 {
				return errors.New("--max-request-bytes must be at least 1 MiB")
			}
			if err := reads.validate(); err != nil {
				return err
			}
			for _, p := range append([]string{cfg.trashDir}, cfg.excludes...) {
				if err := checkName(p); p != "" && err != nil {
					return err
				}
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			printer := ui.NewProgressPrinter(false, gopts.Verbosity, gopts.Term)
			open := func(ctx context.Context, verifier bool) (*repository.Repository, error) {
				opts := *gopts
				if verifier {
					opts.NoCache = true
				}
				repo, err := global.OpenRepository(ctx, opts, printer)
				if err != nil {
					return nil, err
				}
				// The first gated write refreshes its own index. Read endpoints
				// prepare it before loading blobs; inventory needs no index.
				return repo, err
			}
			srv, err := newServeWriteServer(ctx, open, cfg)
			if err != nil {
				return err
			}
			if gopts.NoLock {
				// The caller serializes writes with maintenance itself (for
				// example a control-plane queue and fence). Each write still
				// refreshes its index before it deduplicates.
				srv.lockRepo = noRepositoryLock
			}
			srv.w.content = newContentReader(reads)
			err = serveReadListen(ctx, socket, srv)
			// Requests were canceled with ctx; wait until each write released
			// its repository lock.
			srv.drain()
			if err != nil {
				return err
			}
			return ErrOK
		}}
	cmd.Flags().StringVar(&socket, "socket", "", "platform Unix socket `path` (mode 0600)")
	cmd.Flags().StringArrayVar(&cfg.excludes, "public-exclude", nil, "name `pattern` the public twin leaves out at every depth (path.Match, case-insensitive; repeatable)")
	cmd.Flags().StringVar(&cfg.trashDir, "trash-dir", "", "root directory `name` for the trash, restore and empty-trash edits")
	cmd.Flags().Int64Var(&cfg.maxRequest, "max-request-bytes", 96<<20, "maximum tree-write or verify-write request body `size`")
	reads.addFlags(cmd.Flags())
	return cmd
}

// serveWriteServer routes the socket's requests. Reads use the writer
// engine's repository without a lock. Each write takes upstream's shared
// (append) lock and refreshes the index under it. Verification uses a second
// repository handle with its own index and caches.
type serveWriteServer struct {
	open repoOpener
	cfg  serveWriteConfig
	// w writes and serves reads; v replays and verifies. Their state is used
	// only while holding w.gate, which serializes every request.
	w, v *serveWriteHandler
	// broken is set when an upload failed: upstream's WithBlobUploader keeps
	// its uploader state after an error, so the next write opens the
	// repository again.
	broken bool
	// lockRepo is repository.Lock; tests replace it.
	lockRepo func(ctx context.Context, repo *repository.Repository) (func(), context.Context, error)

	mu       sync.Mutex
	draining bool
	inflight sync.WaitGroup
}

func newServeWriteServer(ctx context.Context, open repoOpener, cfg serveWriteConfig) (*serveWriteServer, error) {
	// Each open derives the key (scrypt); both run at once.
	var repo, vrepo *repository.Repository
	var g errgroup.Group
	g.Go(func() (err error) { repo, err = open(ctx, false); return err })
	g.Go(func() (err error) { vrepo, err = open(ctx, true); return err })
	if err := g.Wait(); err != nil {
		return nil, err
	}
	w := newServeWriteHandler(repo, cfg)
	v := newServeWriteHandler(vrepo, cfg)
	v.verifier = true
	return &serveWriteServer{open: open, cfg: cfg, w: w, v: v, lockRepo: func(ctx context.Context, repo *repository.Repository) (func(), context.Context, error) {
		lock, ctx, err := repository.Lock(ctx, repo, false, 0, func(string) {}, func(string, ...interface{}) {})
		if err != nil {
			return nil, ctx, err
		}
		return lock.Unlock, ctx, nil
	}}, nil
}

// noRepositoryLock is lockRepo under --no-lock: no lock file is written.
func noRepositoryLock(ctx context.Context, _ *repository.Repository) (func(), context.Context, error) {
	return func() {}, ctx, nil
}

// begin registers a write or verify request; it fails once draining started.
func (s *serveWriteServer) begin() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return false
	}
	s.inflight.Add(1)
	return true
}

// drain refuses new requests and waits for the running ones, which release
// their locks before they return.
func (s *serveWriteServer) drain() {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	s.inflight.Wait()
}

type versionResponse struct {
	Protocol  string   `json:"protocol"`
	Version   int      `json:"version"`
	Restic    string   `json:"restic"`
	Endpoints []string `json:"endpoints"`
	TrashDir  string   `json:"trash_dir,omitempty"`
	Excludes  []string `json:"public_excludes"`
	// Features lists additions to protocol version 1 that a caller may use
	// only when the server names them (doc/plori-tree-write.md, Versioning).
	Features []string `json:"features"`
}

// featureMergeSelectors is the selected merge plan: source roles, entry
// selectors, raw byte paths and private names in the head.
const featureMergeSelectors = "merge-source-selectors"

func (s *serveWriteServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/version":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			serveReadError(w, 405)
			return
		}
		writeJSON(w, http.StatusOK, versionResponse{Protocol: "tree-write", Version: treeWriteVersion, Restic: global.Version,
			Endpoints: []string{"/version", "/prepare-write", "/tree-write", "/verify-write", "/prepare", "/tree", "/walk", "/file", "/snapshots", "/skeleton", contentReadPath},
			TrashDir:  s.cfg.trashDir, Excludes: append([]string{}, s.cfg.excludes...), Features: []string{featureMergeSelectors, "public-twin-of-base", "workspace-head-manifest"}})
		return
	case "/prepare-write", "/tree-write", "/verify-write":
	default:
		s.w.serveReadHandler.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		serveReadError(w, 405)
		return
	}
	limit := s.cfg.maxRequest
	if r.URL.Path == "/prepare-write" {
		limit = 4096
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	var prep prepareWriteRequest
	var req treeWriteRequest
	var vreq verifyWriteRequest
	var err error
	switch r.URL.Path {
	case "/prepare-write":
		if err = decodeStrict(r.Body, &prep); err == nil && prep.Version != treeWriteVersion {
			err = errUnsupportedVersion
		}
	case "/tree-write":
		if err = decodeStrict(r.Body, &req); err == nil {
			err = req.validate(s.cfg, true)
		}
	case "/verify-write":
		if err = decodeStrict(r.Body, &vreq); err == nil {
			err = vreq.validate(s.cfg)
		}
	}
	if err != nil {
		writeFailure(w, err)
		return
	}
	if !s.begin() {
		writeJSON(w, http.StatusServiceUnavailable, failureBody{Code: "draining"})
		return
	}
	defer s.inflight.Done()
	gate := s.w.gate
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-r.Context().Done():
		return
	}
	ctx := r.Context()
	if ctx.Err() != nil {
		return
	}
	switch r.URL.Path {
	case "/prepare-write":
		var resp *treeWriteResponse
		if resp, err = s.w.prepareWrite(ctx, prep.Base); err == nil {
			writeJSON(w, http.StatusOK, resp)
			return
		}
	case "/tree-write":
		var resp *treeWriteResponse
		if resp, err = s.treeWrite(ctx, &req); err == nil {
			writeJSON(w, http.StatusOK, resp)
			return
		}
	case "/verify-write":
		var resp *verifyWriteResponse
		if resp, err = s.v.verifyWrite(ctx, &vreq); err == nil {
			writeJSON(w, http.StatusOK, resp)
			return
		}
	}
	writeFailure(w, err)
}

// treeWrite runs one write under its own shared lock. A process that holds no
// lock while idle can see packs a prune removed since the last request, so the
// index is refreshed under the lock before anything is looked up.
func (s *serveWriteServer) treeWrite(ctx context.Context, req *treeWriteRequest) (resp *treeWriteResponse, err error) {
	if s.broken {
		repo, err := s.open(ctx, false)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "serve-write: reopen: %v\n", err)
			return nil, &writeFailureError{status: http.StatusServiceUnavailable, code: "repository_unavailable"}
		}
		s.w.reset(repo)
		s.broken = false
	}
	// The verifier reads through its own handle while the writer acquires its
	// lock and works. A later verify request still refreshes its index, drops
	// statistics if an index disappeared, and replays the exact result.
	join := s.prefetchTwinHead(ctx, req)
	defer func() {
		elapsed := join()
		if resp != nil {
			resp.TimingsMS["head_prefetch"] = float64(elapsed) / float64(time.Millisecond)
		}
	}()
	lockStart := time.Now()
	unlock, lockCtx, err := s.lockRepo(ctx, s.w.repo)
	if err != nil {
		if restic.IsAlreadyLocked(err) {
			return nil, &writeFailureError{status: http.StatusServiceUnavailable, code: "repository_locked"}
		}
		return nil, err
	}
	// The lock is released before the answer, so a caller that has the
	// answer knows this write holds no lock.
	unlocked := false
	defer func() {
		if !unlocked {
			unlock()
		}
	}()
	marks := map[string]float64{"lock": since(lockStart)}
	if err = s.w.refreshIndex(lockCtx, marks); err != nil {
		return nil, err
	}
	resp, err = s.w.write(lockCtx, req)
	if err != nil {
		var uploadErr *uploadError
		if errors.As(err, &uploadErr) {
			s.broken = true
		}
		return nil, err
	}
	unlockStart := time.Now()
	unlocked = true
	unlock()
	marks["unlock"] = since(unlockStart)
	for k, v := range marks {
		resp.TimingsMS[k] = v
	}
	return resp, nil
}

// prefetchTwinHead is speculative read-only work. Its errors do not certify a
// result: verify-write performs the required fresh index and snapshot checks.
// The caller holds the request gate and must join before releasing that gate.
func (s *serveWriteServer) prefetchTwinHead(ctx context.Context, req *treeWriteRequest) func() time.Duration {
	if !req.PublicTwinOfBase || req.Base.Empty {
		return func() time.Duration { return 0 }
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		defer func() { done <- time.Since(start) }()
		s.v.resetCall()
		if s.v.refreshIndex(ctx, map[string]float64{}) != nil {
			return
		}
		_, root, err := s.v.base(ctx, req.Base)
		if err == nil {
			_, _ = s.v.statsOf(ctx, root)
		}
	}()
	return func() time.Duration { cancel(); return <-done }
}

// decodeStrict decodes exactly one JSON value and refuses unknown fields.
func decodeStrict(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return invalidf("body: %v", err)
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return invalidf("body: trailing data")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type failureBody struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

// writeFailureError carries an HTTP status and a fixed code.
type writeFailureError struct {
	status int
	code   string
}

func (e *writeFailureError) Error() string { return e.code }

// uploadError is a failure inside WithBlobUploader.
type uploadError struct{ err error }

func (e *uploadError) Error() string { return "upload: " + e.err.Error() }
func (e *uploadError) Unwrap() error { return e.err }

func writeFailure(w http.ResponseWriter, err error) {
	var refusal *writeRefusal
	var failure *writeFailureError
	switch {
	case errors.As(err, &refusal):
		writeJSON(w, http.StatusConflict, refusal)
	case errors.As(err, &failure):
		writeJSON(w, failure.status, failureBody{Code: failure.code})
	case errors.Is(err, errUnsupportedVersion):
		writeJSON(w, http.StatusBadRequest, failureBody{Code: "unsupported_version"})
	case errors.Is(err, errInvalid):
		writeJSON(w, http.StatusBadRequest, failureBody{Code: "invalid_request", Detail: err.Error()})
	case errors.Is(err, os.ErrNotExist):
		writeJSON(w, http.StatusNotFound, failureBody{Code: "snapshot_not_found"})
	default:
		_, _ = fmt.Fprintf(os.Stderr, "serve-write: %v\n", err)
		writeJSON(w, http.StatusInternalServerError, failureBody{Code: "write_failed"})
	}
}
