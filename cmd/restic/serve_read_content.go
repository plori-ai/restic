package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/restic/restic/internal/bloblru"
	"github.com/restic/restic/internal/crypto"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"
)

// Content-source binding of the lazyfill protocol, version 1
// (doc/plori-lazy-fill.md). POST /v1/read serves byte ranges of content
// blobs, concurrently and without the request gate of the other read
// endpoints.
const (
	contentReadPath      = "/v1/read"
	contentVersionHeader = "Lazyfill-Version"
	contentTimeoutHeader = "Lazyfill-Timeout-Ms"
	contentStatusTrailer = "Lazyfill-Status"
	contentWireVersion   = "1"
	// contentMaxBody bounds a request body. A request lists one span per
	// blob; at restic's average chunk size of about 1 MiB a 16 MiB body
	// covers files of more than 100 GiB.
	contentMaxBody = 16 << 20
	// contentReadAhead is the number of blobs one request fetches ahead of
	// the one it is sending.
	contentReadAhead = 8
)

// contentOptions configure concurrent content reads.
type contentOptions struct {
	// workers bounds concurrent blob loads from the backend.
	workers int
	// memory bounds the bytes of blobs that requests hold at once (fetched
	// or being fetched, not yet sent).
	memory int64
	// cache is the size of the LRU cache of content blobs. It is separate
	// from the tree cache, so content reads do not evict trees.
	cache int
}

func defaultContentOptions() contentOptions {
	return contentOptions{workers: 16, memory: 256 << 20, cache: 32 << 20}
}

func (o *contentOptions) addFlags(f *pflag.FlagSet) {
	f.IntVar(&o.workers, "read-workers", o.workers, "concurrent backend blob loads of content reads")
	f.Int64Var(&o.memory, "read-memory-bytes", o.memory, "bytes of content blobs held by running content reads")
	f.IntVar(&o.cache, "read-cache-bytes", o.cache, "size of the content blob cache")
}

func (o *contentOptions) validate() error {
	if o.workers < 1 || o.memory < 1<<20 || o.cache < 0 {
		return errors.New("--read-workers must be at least 1, --read-memory-bytes at least 1 MiB, --read-cache-bytes not negative")
	}
	return nil
}

// contentReader loads content blobs for /v1/read. Concurrent requests for one
// blob share a single load (a flight); the load runs while at least one
// request waits for it and is cancelled when the last one leaves. Workers
// bound backend loads, memory bounds what requests hold.
type contentReader struct {
	workers chan struct{}
	memory  *semaphore.Weighted
	limit   int64
	cache   *bloblru.Cache

	mu      sync.Mutex
	flights map[restic.ID]*blobFlight
}

type blobFlight struct {
	id     restic.ID
	done   chan struct{}
	blob   []byte
	err    error
	refs   int // guarded by contentReader.mu
	cancel context.CancelFunc
}

func newContentReader(o contentOptions) *contentReader {
	c := &contentReader{workers: make(chan struct{}, o.workers), memory: semaphore.NewWeighted(o.memory), limit: o.memory,
		flights: map[restic.ID]*blobFlight{}}
	if o.cache > 0 {
		c.cache = bloblru.New(o.cache)
	}
	return c
}

// join returns the flight of blob id and holds a reference to it, starting
// the load when none runs. load runs in its own goroutine.
func (c *contentReader) join(id restic.ID, load func(ctx context.Context) ([]byte, error)) *blobFlight {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fl, ok := c.flights[id]; ok {
		fl.refs++
		return fl
	}
	if c.cache != nil {
		if blob, ok := c.cache.Get(id); ok {
			fl := &blobFlight{id: id, done: make(chan struct{}), blob: blob, refs: 1, cancel: func() {}}
			close(fl.done)
			return fl
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	fl := &blobFlight{id: id, done: make(chan struct{}), refs: 1, cancel: cancel}
	c.flights[id] = fl
	go c.fetch(ctx, fl, load)
	return fl
}

func (c *contentReader) fetch(ctx context.Context, fl *blobFlight, load func(ctx context.Context) ([]byte, error)) {
	var blob []byte
	var err error
	select {
	case c.workers <- struct{}{}:
		blob, err = load(ctx)
		<-c.workers
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err == nil && c.cache != nil {
		c.cache.Add(fl.id, blob)
	}
	c.mu.Lock()
	if c.flights[fl.id] == fl {
		delete(c.flights, fl.id)
	}
	fl.blob, fl.err = blob, err
	close(fl.done)
	c.mu.Unlock()
	fl.cancel()
}

// leave drops a reference; the last one cancels a load that still runs.
func (c *contentReader) leave(fl *blobFlight) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fl.refs--
	if fl.refs == 0 {
		if c.flights[fl.id] == fl {
			delete(c.flights, fl.id)
		}
		fl.cancel()
	}
}

func (fl *blobFlight) wait(ctx context.Context) ([]byte, error) {
	select {
	case <-fl.done:
		return fl.blob, fl.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// contentRequest is the JSON body of POST /v1/read.
type contentRequest struct {
	Source   string        `json:"source"`
	Path     string        `json:"path,omitempty"`
	FileSize uint64        `json:"file_size"`
	Offset   uint64        `json:"offset"`
	Length   uint64        `json:"length"`
	Spans    []contentSpan `json:"spans,omitempty"`
}

type contentSpan struct {
	Blob   string `json:"blob"`
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// contentPart is one resolved span: a blob of known plaintext size and the
// range of it to send.
type contentPart struct {
	id                   restic.ID
	size, offset, length uint64
}

// contentError is a classified failure of the content-source binding.
type contentError struct {
	code    string
	message string
}

func (e *contentError) Error() string { return e.code + ": " + e.message }

func contentErrorf(code, format string, args ...any) *contentError {
	return &contentError{code: code, message: fmt.Sprintf(format, args...)}
}

var contentStatus = map[string]int{
	"bad_request": http.StatusBadRequest, "unsupported_version": http.StatusBadRequest, "denied": http.StatusForbidden,
	"not_found": http.StatusNotFound, "corrupt": http.StatusBadGateway, "unavailable": http.StatusServiceUnavailable,
	"deadline": http.StatusGatewayTimeout, "internal": http.StatusInternalServerError,
}

// classifyContentError maps a failure to a binding code. The request
// context decides deadline; repository errors are not typed beyond
// authentication, so other load failures are reported as unavailable.
func classifyContentError(ctx context.Context, err error) *contentError {
	var ce *contentError
	switch {
	case errors.As(err, &ce):
		return ce
	case ctx.Err() != nil:
		return &contentError{code: "deadline", message: ctx.Err().Error()}
	case errors.Is(err, crypto.ErrUnauthenticated):
		return &contentError{code: "corrupt", message: err.Error()}
	default:
		return &contentError{code: "unavailable", message: err.Error()}
	}
}

// statusText makes a message safe for an HTTP trailer value.
func statusText(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r >= 0x80 {
			return '?'
		}
		return r
	}, s)
}

func writeContentError(w http.ResponseWriter, e *contentError) {
	writeJSON(w, contentStatus[e.code], struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{e.code, e.message})
}

// serveContent handles POST /v1/read.
func (s *serveReadHandler) serveContent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		serveReadError(w, http.StatusMethodNotAllowed)
		return
	}
	if v := r.Header.Get(contentVersionHeader); v != contentWireVersion {
		writeContentError(w, contentErrorf("unsupported_version", "version %q", v))
		return
	}
	ctx := r.Context()
	if h := r.Header.Get(contentTimeoutHeader); h != "" {
		ms, err := strconv.ParseInt(h, 10, 64)
		if err != nil || ms <= 0 {
			writeContentError(w, contentErrorf("bad_request", "%s %q", contentTimeoutHeader, h))
			return
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
		defer cancel()
	}
	var req contentRequest
	if err := decodeStrict(http.MaxBytesReader(w, r.Body, contentMaxBody), &req); err != nil {
		writeContentError(w, contentErrorf("bad_request", "%v", err))
		return
	}
	parts, err := s.resolveContent(ctx, &req)
	if err != nil {
		writeContentError(w, classifyContentError(ctx, err))
		return
	}
	s.sendContent(ctx, w, parts)
}

// resolveContent validates a request and maps it to blob ranges. With spans,
// the spans are served as given after checking them against the index; a
// request without spans names a file by its raw path in the snapshot.
func (s *serveReadHandler) resolveContent(ctx context.Context, req *contentRequest) ([]contentPart, error) {
	source, err := restic.ParseID(req.Source)
	if err != nil || req.Source != source.String() {
		return nil, contentErrorf("bad_request", "source must be a full lower-case snapshot ID")
	}
	if req.Offset > req.FileSize || req.Length > req.FileSize-req.Offset {
		return nil, contentErrorf("bad_request", "range [%d,+%d) outside file size %d", req.Offset, req.Length, req.FileSize)
	}
	if len(req.Spans) == 0 {
		if req.Path == "" {
			if req.Length == 0 {
				return nil, nil
			}
			return nil, contentErrorf("bad_request", "request names neither spans nor a path")
		}
		return s.resolvePath(ctx, source, req)
	}
	parts := make([]contentPart, 0, len(req.Spans))
	var sum uint64
	for _, sp := range req.Spans {
		id, err := restic.ParseID(sp.Blob)
		if err != nil || sp.Blob != id.String() || sp.Length == 0 {
			return nil, contentErrorf("bad_request", "span blob %q length %d", sp.Blob, sp.Length)
		}
		parts = append(parts, contentPart{id: id, offset: sp.Offset, length: sp.Length})
		sum += sp.Length
		if sum < sp.Length {
			return nil, contentErrorf("bad_request", "span lengths overflow")
		}
	}
	if sum != req.Length {
		return nil, contentErrorf("bad_request", "spans sum to %d, length is %d", sum, req.Length)
	}
	if err := s.blobSizes(ctx, parts); err != nil {
		return nil, err
	}
	for _, p := range parts {
		if p.offset > p.size || p.length > p.size-p.offset {
			return nil, contentErrorf("bad_request", "span [%d,+%d) outside blob %s of %d bytes", p.offset, p.length, p.id, p.size)
		}
	}
	return parts, nil
}

// blobSizes sets the plaintext size of every part from the index. A blob the
// index lacks causes one index reload before it is reported as not found.
func (s *serveReadHandler) blobSizes(ctx context.Context, parts []contentPart) error {
	lookup := func(repo *repository.Repository) error {
		for i := range parts {
			n, ok := repo.LookupBlobSize(restic.DataBlob, parts[i].id)
			if !ok {
				return contentErrorf("not_found", "blob %s", parts[i].id)
			}
			parts[i].size = uint64(n)
		}
		return nil
	}
	return s.withIndexRetry(ctx, lookup)
}

// resolvePath finds a regular file by its raw path and maps the range onto
// its content blobs.
func (s *serveReadHandler) resolvePath(ctx context.Context, source restic.ID, req *contentRequest) ([]contentPart, error) {
	raw, err := base64.StdEncoding.DecodeString(req.Path)
	if err != nil {
		return nil, contentErrorf("bad_request", "path is not standard base64")
	}
	if err := checkRawPath(raw); err != nil || len(raw) == 0 {
		return nil, contentErrorf("bad_request", "path is not a canonical relative path")
	}
	root, err := s.sourceRoot(ctx, source)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, contentErrorf("not_found", "snapshot %s", source)
		}
		return nil, err
	}
	loader := directLoader{s}
	node, err := findRawPath(ctx, loader, root, string(raw))
	if err != nil {
		return nil, err
	}
	if node.Type != data.NodeTypeFile {
		return nil, contentErrorf("not_found", "path is a %s, not a regular file", node.Type)
	}
	if node.Size != req.FileSize {
		return nil, contentErrorf("bad_request", "file_size %d, snapshot file has %d bytes", req.FileSize, node.Size)
	}
	parts := make([]contentPart, len(node.Content))
	for i, id := range node.Content {
		parts[i].id = id
	}
	if err := s.blobSizes(ctx, parts); err != nil {
		return nil, err
	}
	var total uint64
	for _, p := range parts {
		total += p.size
	}
	if total != node.Size {
		return nil, contentErrorf("corrupt", "content blobs sum to %d, file size is %d", total, node.Size)
	}
	// Keep the parts that overlap [offset, offset+length).
	out := parts[:0]
	pos, end := uint64(0), req.Offset+req.Length
	for _, p := range parts {
		pEnd := pos + p.size
		if pEnd > req.Offset && pos < end {
			from, to := max(req.Offset, pos), min(end, pEnd)
			out = append(out, contentPart{id: p.id, size: p.size, offset: from - pos, length: to - from})
		}
		pos = pEnd
	}
	return out, nil
}

// findRawPath resolves a relative path below root without following links.
func findRawPath(ctx context.Context, loader restic.BlobLoader, root restic.ID, p string) (*data.Node, error) {
	tree := root
	parts := strings.Split(p, "/")
	for i, name := range parts {
		var found *data.Node
		nodes, err := data.LoadTree(ctx, loader, tree)
		if err != nil {
			return nil, err
		}
		for item := range nodes {
			if item.Error != nil {
				return nil, item.Error
			}
			if item.Node.Name == name {
				found = item.Node
				break
			}
		}
		if found == nil {
			return nil, contentErrorf("not_found", "path not in snapshot")
		}
		if i == len(parts)-1 {
			return found, nil
		}
		if found.Type != data.NodeTypeDir || found.Subtree == nil {
			return nil, contentErrorf("not_found", "path not in snapshot")
		}
		tree = *found.Subtree
	}
	panic("unreachable")
}

// sendContent streams the parts in order. The response header goes out with
// the first byte, so a failure before it is still a classified JSON error;
// a failure after it ends the body with an error status trailer.
func (s *serveReadHandler) sendContent(ctx context.Context, w http.ResponseWriter, parts []contentPart) {
	c := s.content
	started := false
	start := func() {
		started = true
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Trailer", contentStatusTrailer)
		w.WriteHeader(http.StatusOK)
	}
	fail := func(err error) {
		ce := classifyContentError(ctx, err)
		if !started {
			writeContentError(w, ce)
			return
		}
		w.Header().Set(contentStatusTrailer, statusText("error "+ce.code+" "+ce.message))
	}
	// Flights and memory reservations of parts [next, ahead).
	flights := make([]*blobFlight, len(parts))
	reserved := make([]int64, len(parts))
	next, ahead := 0, 0
	defer func() {
		for i := next; i < ahead; i++ {
			c.leave(flights[i])
			c.memory.Release(reserved[i])
		}
	}()
	weight := func(p contentPart) int64 { return min(int64(p.size), c.limit) } //nolint:gosec // bounded by limit
	rc := http.NewResponseController(w)
	for next < len(parts) {
		// A request waits for memory only while it holds none, so requests
		// never wait on each other's reservations; read-ahead takes only
		// what is free.
		if ahead == next {
			if err := c.memory.Acquire(ctx, weight(parts[next])); err != nil {
				fail(err)
				return
			}
			reserved[ahead] = weight(parts[ahead])
			flights[ahead] = s.joinBlob(parts[ahead])
			ahead++
		}
		for ahead < len(parts) && ahead-next < contentReadAhead && c.memory.TryAcquire(weight(parts[ahead])) {
			reserved[ahead] = weight(parts[ahead])
			flights[ahead] = s.joinBlob(parts[ahead])
			ahead++
		}
		p := parts[next]
		blob, err := flights[next].wait(ctx)
		if err == nil && uint64(len(blob)) != p.size {
			err = contentErrorf("corrupt", "blob %s has %d bytes, index says %d", p.id, len(blob), p.size)
		}
		if err != nil {
			fail(err)
			return
		}
		if !started {
			start()
		}
		_, werr := w.Write(blob[p.offset : p.offset+p.length])
		c.leave(flights[next])
		c.memory.Release(reserved[next])
		next++
		if werr == nil {
			werr = rc.Flush()
		}
		if werr != nil {
			return
		}
	}
	if !started {
		start()
	}
	w.Header().Set(contentStatusTrailer, "ok")
}

func (s *serveReadHandler) joinBlob(p contentPart) *blobFlight {
	return s.content.join(p.id, func(ctx context.Context) ([]byte, error) {
		var blob []byte
		err := s.withIndexRetry(ctx, func(repo *repository.Repository) (err error) {
			blob, err = repo.LoadBlob(ctx, restic.DataBlob, p.id, nil)
			return err
		})
		return blob, err
	})
}

// directLoader loads tree blobs from the repository without the shared tree
// cache, retrying once after an index reload. It is safe for concurrent use.
type directLoader struct{ s *serveReadHandler }

func (l directLoader) LoadBlob(ctx context.Context, typ restic.BlobType, id restic.ID, _ []byte) ([]byte, error) {
	var blob []byte
	err := l.s.withIndexRetry(ctx, func(repo *repository.Repository) (err error) {
		blob, err = repo.LoadBlob(ctx, typ, id, nil)
		return err
	})
	return blob, err
}

// handle returns the current repository handle. serve-write replaces it after
// a failed upload; readers that hold the old one finish with it.
func (s *serveReadHandler) handle() *repository.Repository {
	s.repoMu.RLock()
	defer s.repoMu.RUnlock()
	return s.repo
}

// withIndexRetry runs fn and, when it fails while ctx is live, reloads the
// index once and runs it again. The index loaded at start-up does not know
// packs written later, and after a prune it names packs that are gone.
//
// Loading the index is not safe concurrently with a write's upload or with
// another load, so the reload takes the request gate that serializes those
// (the gated endpoints and every serve-write request). Readers whose lookups
// missed before the same reload share it through the index generation. A
// reader that misses while a write holds the gate waits for that write, within
// its own deadline. fn runs outside the gate.
func (s *serveReadHandler) withIndexRetry(ctx context.Context, fn func(*repository.Repository) error) error {
	gen := s.indexGen.Load()
	err := fn(s.handle())
	if err == nil || ctx.Err() != nil {
		return err
	}
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.indexGen.Load() == gen {
		if err := s.repo.LoadIndex(ctx, nil); err != nil {
			<-s.gate
			return err
		}
		s.indexGen.Add(1)
	}
	<-s.gate
	return fn(s.handle())
}

// sourceRoot returns the root tree of a snapshot. Roots are cached for the
// life of the process; a snapshot is immutable.
func (s *serveReadHandler) sourceRoot(ctx context.Context, id restic.ID) (restic.ID, error) {
	s.sourcesMu.Lock()
	root, ok := s.sources[id]
	s.sourcesMu.Unlock()
	if ok {
		return root, nil
	}
	repo := s.handle()
	sn, err := data.LoadSnapshot(ctx, repo, id)
	if err != nil {
		if ctx.Err() != nil {
			return restic.ID{}, ctx.Err()
		}
		found := false
		listErr := repo.List(ctx, restic.SnapshotFile, func(candidate restic.ID, _ int64) error {
			found = found || candidate == id
			return nil
		})
		if listErr == nil && !found {
			return restic.ID{}, os.ErrNotExist
		}
		return restic.ID{}, err
	}
	if sn.Tree == nil {
		return restic.ID{}, errors.New("snapshot has no tree")
	}
	s.sourcesMu.Lock()
	if len(s.sources) >= 1024 {
		clear(s.sources)
	}
	s.sources[id] = *sn.Tree
	s.sourcesMu.Unlock()
	return *sn.Tree, nil
}
