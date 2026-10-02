package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/mem"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// contentEnv is a memory repository with a generated snapshot behind a
// latency backend, served by a serve-read handler on a Unix socket.
type contentEnv struct {
	be     *latencyBackend
	loads  int64
	repo   *repository.Repository
	h      *serveReadHandler
	socket string
	id     restic.ID
	blobs  map[restic.ID][]byte
	files  []lzfmTestRecord // regular files with content
}

func newContentEnv(t *testing.T, spec genTreeSpec, opts contentOptions) *contentEnv {
	writer, raw := repository.TestRepositoryWithBackend(t, mem.New(), 0, repository.Options{})
	e := &contentEnv{}
	e.id, e.blobs = genTree(t, writer, spec)
	e.be = &latencyBackend{Backend: raw, count: &e.loads}
	e.repo = repository.TestOpenBackend(t, e.be)
	rtest.OK(t, e.repo.LoadIndex(context.Background(), nil))
	e.h = newServeReadHandler(e.repo)
	e.h.content = newContentReader(opts)
	e.socket = serveOnSocket(t, e.h)
	b, _ := fetchSkeleton(t, e.socket, e.id)
	st, err := lzfmDecode(bytes.NewReader(b))
	rtest.OK(t, err)
	for _, r := range st.Records {
		if r.Type == lzfmFile && r.Size > 0 {
			e.files = append(e.files, r)
		}
	}
	atomic.StoreInt64(&e.loads, 0)
	return e
}

// want is the content of a file record from the generator's blobs.
func (e *contentEnv) want(r lzfmTestRecord) []byte {
	var b []byte
	for _, x := range r.Blobs {
		b = append(b, e.blobs[restic.ID(x.ID)]...)
	}
	return b
}

func (e *contentEnv) read(ctx context.Context, r lzfmTestRecord, off, length uint64, timeout time.Duration) ([]byte, error) {
	return contentRead(ctx, e.socket, contentReadReq{Source: e.id, Path: r.Path, FileSize: r.Size, Blobs: r.Blobs, Offset: off, Length: length, Timeout: timeout})
}

func readCode(err error) string {
	var re *contentReadError
	if errors.As(err, &re) {
		return re.Code
	}
	return fmt.Sprintf("unclassified %v", err)
}

func postContent(h http.Handler, version string, body any) (int, string) {
	js, _ := json.Marshal(body)
	r, _ := http.NewRequest(http.MethodPost, contentReadPath, bytes.NewReader(js))
	if version != "" {
		r.Header.Set(contentVersionHeader, version)
	}
	w := serveReadRequestRaw(h, r)
	var e struct{ Code string }
	_ = json.Unmarshal(w.Body.Bytes(), &e)
	return w.Code, e.Code
}

func TestServeReadContentRangesAndErrors(t *testing.T) {
	e := newContentEnv(t, genTreeSpec{top: 2, mid: 2, leaf: 60, seed: 1, blobs: 48, maxBlob: 96 << 10}, defaultContentOptions())
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(1))
	var multi, raw *lzfmTestRecord
	for i := range e.files {
		f := &e.files[i]
		if len(f.Blobs) > 1 && multi == nil {
			multi = f
		}
		if strings.Contains(string(f.Path), "\xff") && raw == nil {
			raw = f
		}
	}
	if multi == nil || raw == nil {
		t.Fatal("fixture lacks a multi-blob or a non-UTF-8 file")
	}
	// Whole files, ranges across blob boundaries, zero length at EOF; by
	// blobs and by raw path.
	for _, f := range []*lzfmTestRecord{multi, raw, &e.files[0], &e.files[len(e.files)-1]} {
		want := e.want(*f)
		ranges := [][2]uint64{{0, f.Size}, {f.Size, 0}, {0, 1}, {f.Size - 1, 1}}
		for k := 0; k < 20; k++ {
			off := uint64(rnd.Int63n(int64(f.Size)))
			ranges = append(ranges, [2]uint64{off, uint64(rnd.Int63n(int64(f.Size - off + 1)))})
		}
		for _, rg := range ranges {
			got, err := e.read(ctx, *f, rg[0], rg[1], time.Second)
			rtest.OK(t, err)
			rtest.Assert(t, bytes.Equal(want[rg[0]:rg[0]+rg[1]], got), "%q [%d,+%d) by blobs", f.Path, rg[0], rg[1])
			byPath := *f
			byPath.Blobs = nil
			got, err = e.read(ctx, byPath, rg[0], rg[1], time.Second)
			rtest.OK(t, err)
			rtest.Assert(t, bytes.Equal(want[rg[0]:rg[0]+rg[1]], got), "%q [%d,+%d) by path", f.Path, rg[0], rg[1])
		}
	}

	// Refusals before the first byte.
	span := func(id []byte, off, length uint64) contentSpan {
		return contentSpan{Blob: fmt.Sprintf("%x", id), Offset: off, Length: length}
	}
	b0 := multi.Blobs[0]
	src := e.id.String()
	for name, c := range map[string]struct {
		version string
		body    any
		status  int
		code    string
	}{
		"version":         {"2", contentRequest{Source: src}, 400, "unsupported_version"},
		"no version":      {"", contentRequest{Source: src}, 400, "unsupported_version"},
		"unknown field":   {"1", map[string]any{"source": src, "extra": 1}, 400, "bad_request"},
		"short source":    {"1", contentRequest{Source: e.id.Str()}, 400, "bad_request"},
		"upper source":    {"1", contentRequest{Source: strings.ToUpper(src)}, 400, "bad_request"},
		"span sum":        {"1", contentRequest{Source: src, FileSize: 10, Length: 2, Spans: []contentSpan{span(b0.ID, 0, 1)}}, 400, "bad_request"},
		"span past blob":  {"1", contentRequest{Source: src, FileSize: b0.Length + 1, Length: 2, Spans: []contentSpan{span(b0.ID, b0.Length-1, 2)}}, 400, "bad_request"},
		"range past size": {"1", contentRequest{Source: src, FileSize: 1, Length: 2, Spans: []contentSpan{span(b0.ID, 0, 2)}}, 400, "bad_request"},
		"missing blob":    {"1", contentRequest{Source: src, FileSize: 1, Length: 1, Spans: []contentSpan{span(make([]byte, 32), 0, 1)}}, 404, "not_found"},
		"missing path": {"1", contentRequest{Source: src, FileSize: 1, Length: 1,
			Path: base64.StdEncoding.EncodeToString([]byte("t0000/nope"))}, 404, "not_found"},
		"directory path": {"1", contentRequest{Source: src, FileSize: 1, Length: 1,
			Path: base64.StdEncoding.EncodeToString([]byte("t0000/m0000"))}, 404, "not_found"},
		"dotdot path": {"1", contentRequest{Source: src, FileSize: 1, Length: 1,
			Path: base64.StdEncoding.EncodeToString([]byte("t0000/../x"))}, 400, "bad_request"},
		"file size": {"1", contentRequest{Source: src, FileSize: multi.Size + 1, Length: 1,
			Path: base64.StdEncoding.EncodeToString(multi.Path)}, 400, "bad_request"},
		"missing snapshot": {"1", contentRequest{Source: restic.NewRandomID().String(), FileSize: 1, Length: 1,
			Path: base64.StdEncoding.EncodeToString(multi.Path)}, 404, "not_found"},
		"nothing": {"1", contentRequest{Source: src, FileSize: 1, Length: 1}, 400, "bad_request"},
	} {
		status, code := postContent(e.h, c.version, c.body)
		rtest.Equals(t, [2]any{c.status, c.code}, [2]any{status, code}, name)
	}
	w := serveReadRequest(e.h, http.MethodGet, contentReadPath, "")
	rtest.Equals(t, http.StatusMethodNotAllowed, w.Code)
	r, _ := http.NewRequest(http.MethodPost, contentReadPath, strings.NewReader("{}"))
	r.Header.Set(contentVersionHeader, "1")
	r.Header.Set(contentTimeoutHeader, "soon")
	rtest.Equals(t, http.StatusBadRequest, serveReadRequestRaw(e.h, r).Code)
}

func serveReadRequestRaw(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// TestServeReadContentConcurrent64 runs 64 concurrent readers over the files
// of a 50k-entry snapshot whose backend answers each pack read after 20 ms,
// with a 2 s deadline per read: no read may fail or miss its deadline.
func TestServeReadContentConcurrent64(t *testing.T) {
	e := newContentEnv(t, genTreeSpec{top: 20, mid: 25, leaf: 100, seed: 64, blobs: 2048, maxBlob: 256 << 10}, defaultContentOptions())
	e.be.delay = 20 * time.Millisecond
	const readers, perReader, deadline = 64, 40, 2 * time.Second
	var mu sync.Mutex
	var lat []time.Duration
	var bytesRead int64
	var failures []string
	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < readers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(g)))
			for k := 0; k < perReader; k++ {
				f := e.files[rnd.Intn(len(e.files))]
				off, length := uint64(0), f.Size // a fill reads [0, owed)
				if k%4 == 3 {
					off = uint64(rnd.Int63n(int64(f.Size)))
					length = uint64(rnd.Int63n(int64(f.Size-off))) + 1
				}
				ctx, cancel := context.WithTimeout(context.Background(), deadline)
				t0 := time.Now()
				got, err := e.read(ctx, f, off, length, deadline)
				d := time.Since(t0)
				cancel()
				mu.Lock()
				lat = append(lat, d)
				switch {
				case err != nil:
					failures = append(failures, fmt.Sprintf("%q: %v", f.Path, err))
				case !bytes.Equal(e.want(f)[off:off+length], got):
					failures = append(failures, fmt.Sprintf("%q: wrong bytes", f.Path))
				default:
					bytesRead += int64(length)
				}
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	wall := time.Since(start)
	rtest.Assert(t, len(failures) == 0, "%d of %d reads failed: %v", len(failures), readers*perReader, failures[:min(len(failures), 5)])
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration { return lat[int(p*float64(len(lat)-1))].Round(time.Millisecond) }
	t.Logf("%d reads by %d readers, %d MiB in %v: p50 %v p95 %v p99 %v max %v; %d backend pack reads",
		len(lat), readers, bytesRead>>20, wall.Round(time.Millisecond), pct(0.5), pct(0.95), pct(0.99), lat[len(lat)-1].Round(time.Millisecond), atomic.LoadInt64(&e.loads))
	rtest.Assert(t, lat[len(lat)-1] < deadline, "slowest read %v", lat[len(lat)-1])
}

// TestServeReadContentIsNotSerialized reads 64 distinct blobs at once from a
// backend that answers after 50 ms. Serialized reads take 3.2 s; 16 workers
// take about 0.2 s.
func TestServeReadContentIsNotSerialized(t *testing.T) {
	e := newContentEnv(t, genTreeSpec{top: 1, mid: 4, leaf: 60, seed: 2, blobs: 256, maxBlob: 4 << 10}, defaultContentOptions())
	e.be.delay = 50 * time.Millisecond
	// One file per distinct first blob.
	seen := map[string]bool{}
	var files []lzfmTestRecord
	for _, f := range e.files {
		if !seen[string(f.Blobs[0].ID)] && len(files) < 64 {
			seen[string(f.Blobs[0].ID)] = true
			files = append(files, lzfmTestRecord{Path: f.Path, Size: f.Blobs[0].Length, Blobs: f.Blobs[:1]})
		}
	}
	rtest.Equals(t, 64, len(files))
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, len(files))
	for _, f := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.read(context.Background(), f, 0, f.Size, 5*time.Second)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		rtest.OK(t, err)
	}
	wall := time.Since(start)
	t.Logf("64 single-blob reads with 50 ms backend latency: %v, %d pack reads", wall.Round(time.Millisecond), atomic.LoadInt64(&e.loads))
	rtest.Assert(t, wall < 1600*time.Millisecond, "64 reads took %v", wall)
}

// TestServeReadContentCoalesces reads one blob from 32 requests while the
// backend is blocked: one backend read serves all, and a request that leaves
// does not fail the others. When every request leaves, the read is cancelled
// and a later request starts a new one.
func TestServeReadContentCoalesces(t *testing.T) {
	opts := defaultContentOptions()
	opts.cache = 0 // every flight goes to the backend
	e := newContentEnv(t, genTreeSpec{top: 1, mid: 1, leaf: 40, seed: 3, blobs: 16, maxBlob: 64 << 10}, opts)
	f := e.files[0]
	f = lzfmTestRecord{Path: f.Path, Size: f.Blobs[0].Length, Blobs: f.Blobs[:1]}
	id := restic.ID(f.Blobs[0].ID)
	refs := func() int {
		e.h.content.mu.Lock()
		defer e.h.content.mu.Unlock()
		if fl, ok := e.h.content.flights[id]; ok {
			return fl.refs
		}
		return 0
	}
	waitFor := func(cond func() bool) {
		deadline := time.Now().Add(5 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatal("condition not reached")
			}
			time.Sleep(time.Millisecond)
		}
	}
	e.be.block = make(chan struct{})
	quitter, quit := context.WithCancel(context.Background())
	results := make(chan error, 32)
	for i := 0; i < 32; i++ {
		ctx := context.Background()
		if i == 0 {
			ctx = quitter
		}
		go func() {
			got, err := e.read(ctx, f, 0, f.Size, 10*time.Second)
			if err == nil && !bytes.Equal(got, e.want(f)) {
				err = errors.New("wrong bytes")
			}
			results <- err
		}()
	}
	waitFor(func() bool { return refs() == 32 })
	quit()
	waitFor(func() bool { return refs() == 31 })
	close(e.be.block)
	failed := 0
	for i := 0; i < 32; i++ {
		if err := <-results; err != nil {
			failed++
		}
	}
	rtest.Equals(t, 1, failed) // the request that left
	rtest.Equals(t, int64(1), atomic.LoadInt64(&e.loads))

	// Every request leaves: the backend read is cancelled, the next request
	// loads again.
	e.be.block = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := e.read(ctx, f, 0, f.Size, 10*time.Second); done <- err }()
	waitFor(func() bool { return refs() == 1 && atomic.LoadInt64(&e.loads) == 2 })
	cancel()
	rtest.Assert(t, <-done != nil, "cancelled read succeeded")
	waitFor(func() bool { return refs() == 0 })
	close(e.be.block)
	got, err := e.read(context.Background(), f, 0, f.Size, 10*time.Second)
	rtest.OK(t, err)
	rtest.Assert(t, bytes.Equal(got, e.want(f)), "wrong bytes after a cancelled flight")
	rtest.Equals(t, int64(3), atomic.LoadInt64(&e.loads))
}

// TestServeReadContentDeadline checks both deadline paths: before the first
// byte the answer is 504 deadline; after it, the status trailer says deadline.
func TestServeReadContentDeadline(t *testing.T) {
	e := newContentEnv(t, genTreeSpec{top: 1, mid: 1, leaf: 40, seed: 4, blobs: 16, maxBlob: 64 << 10}, defaultContentOptions())
	var f lzfmTestRecord
	for _, c := range e.files {
		if len(c.Blobs) >= 2 && !bytes.Equal(c.Blobs[0].ID, c.Blobs[1].ID) {
			f = c
			break
		}
	}
	rtest.Assert(t, f.Size > 0, "no file with two distinct blobs")
	first := lzfmTestRecord{Path: f.Path, Size: f.Blobs[0].Length, Blobs: f.Blobs[:1]}
	_, err := e.read(context.Background(), first, 0, first.Size, time.Second) // now cached
	rtest.OK(t, err)
	e.be.block = make(chan struct{})
	defer close(e.be.block)

	start := time.Now()
	second := lzfmTestRecord{Path: f.Path, Size: f.Blobs[1].Length, Blobs: f.Blobs[1:2]}
	_, err = e.read(context.Background(), second, 0, second.Size, 300*time.Millisecond)
	took := time.Since(start)
	rtest.Equals(t, "deadline", readCode(err))
	rtest.Assert(t, took >= 290*time.Millisecond && took < 800*time.Millisecond, "deadline answer after %v", took)

	start = time.Now()
	_, err = e.read(context.Background(), f, 0, f.Size, 300*time.Millisecond)
	took = time.Since(start)
	rtest.Equals(t, "deadline", readCode(err))
	rtest.Assert(t, took >= 290*time.Millisecond && took < 800*time.Millisecond, "deadline trailer after %v", took)
}

// TestServeReadContentSmallMemoryBudget runs many multi-blob reads with a
// budget smaller than their read-ahead: they wait for memory, never for
// each other's reservations, and all complete.
func TestServeReadContentSmallMemoryBudget(t *testing.T) {
	opts := contentOptions{workers: 4, memory: 1 << 20, cache: 0}
	e := newContentEnv(t, genTreeSpec{top: 1, mid: 2, leaf: 60, seed: 5, blobs: 64, maxBlob: 512 << 10}, opts)
	e.be.delay = 2 * time.Millisecond
	var wg sync.WaitGroup
	var failed atomic.Int64
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 8; k++ {
				f := e.files[(g*8+k)%len(e.files)]
				got, err := e.read(context.Background(), f, 0, f.Size, 20*time.Second)
				if err != nil || !bytes.Equal(got, e.want(f)) {
					failed.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	rtest.Equals(t, int64(0), failed.Load())
}

// countingIndexBackend counts index listings, which mark index reloads.
type countingIndexBackend struct {
	backend.Backend
	lists atomic.Int64
}

func (b *countingIndexBackend) List(ctx context.Context, t backend.FileType, fn func(backend.FileInfo) error) error {
	if t == backend.IndexFile {
		b.lists.Add(1)
	}
	return b.Backend.List(ctx, t, fn)
}

// TestServeReadContentReloadsIndex reads blobs of a snapshot written after
// the reader loaded its index: 64 concurrent misses share index reloads.
// A blob that stays missing is not_found after one reload.
func TestServeReadContentReloadsIndex(t *testing.T) {
	writer, raw := repository.TestRepositoryWithBackend(t, mem.New(), 0, repository.Options{})
	be := &countingIndexBackend{Backend: raw}
	reader := repository.TestOpenBackend(t, be)
	rtest.OK(t, reader.LoadIndex(context.Background(), nil))
	h := newServeReadHandler(reader)
	socket := serveOnSocket(t, h)
	id, blobs := genTree(t, writer, genTreeSpec{top: 1, mid: 2, leaf: 60, seed: 6, blobs: 64, maxBlob: 8 << 10})
	var ids restic.IDs
	for b := range blobs {
		ids = append(ids, b)
	}
	before := be.lists.Load()
	var wg sync.WaitGroup
	var failed atomic.Int64
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(b restic.ID) {
			defer wg.Done()
			n := uint64(len(blobs[b]))
			got, err := contentRead(context.Background(), socket, contentReadReq{Source: id, FileSize: n, Length: n,
				Blobs: []lzfmTestBlob{{ID: b[:], Length: n}}, Timeout: 5 * time.Second})
			if err != nil || !bytes.Equal(got, blobs[b]) {
				failed.Add(1)
			}
		}(ids[i%len(ids)])
	}
	wg.Wait()
	rtest.Equals(t, int64(0), failed.Load())
	reloads := be.lists.Load() - before
	t.Logf("64 concurrent misses caused %d index listings", reloads)
	rtest.Assert(t, reloads >= 1 && reloads <= 4, "%d index listings", reloads)

	missing := restic.NewRandomID()
	before = be.lists.Load()
	_, err := contentRead(context.Background(), socket, contentReadReq{Source: id, FileSize: 1, Length: 1, Blobs: []lzfmTestBlob{{ID: missing[:], Length: 1}}})
	rtest.Equals(t, "not_found", readCode(err))
	rtest.Equals(t, int64(1), be.lists.Load()-before)

	// The skeleton of the new snapshot lists every blob size.
	b, _ := fetchSkeleton(t, socket, id)
	_, err = lzfmDecode(bytes.NewReader(b))
	rtest.OK(t, err)
}

// TestServeReadContentDuringWrites runs content reads and skeletons on a
// serve-write socket while tree writes run, including reads of the
// snapshots the writes produce. Run with -race.
func TestServeReadContentDuringWrites(t *testing.T) {
	f := newSWFixture(t)
	base := f.backup(nil, false)
	socket := serveOnSocket(t, f.srv)
	ctx := context.Background()
	b, _ := fetchSkeleton(t, socket, base)
	st, err := lzfmDecode(bytes.NewReader(b))
	rtest.OK(t, err)
	var files []lzfmTestRecord
	for _, r := range st.Records {
		if r.Type == lzfmFile && r.Size > 0 {
			files = append(files, r)
		}
	}
	rtest.Assert(t, len(files) > 0, "no files")
	stop := make(chan struct{})
	var heads []restic.ID
	var headsMu sync.Mutex
	var wg sync.WaitGroup
	var reads, failed atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; ; k++ {
				select {
				case <-stop:
					return
				default:
				}
				if k%5 == 4 {
					headsMu.Lock()
					src := base
					if len(heads) > 0 {
						src = heads[len(heads)-1]
					}
					headsMu.Unlock()
					resp, err := unixHTTPClient(socket).Get("http://serve-write/skeleton?snapshot=" + src.String())
					if err == nil {
						_, err = lzfmDecode(resp.Body)
						_ = resp.Body.Close()
					}
					if err != nil {
						failed.Add(1)
					}
					continue
				}
				r := files[(g+k)%len(files)]
				got, err := contentRead(ctx, socket, contentReadReq{Source: base, Path: r.Path, FileSize: r.Size, Blobs: r.Blobs, Length: r.Size, Timeout: 5 * time.Second})
				reads.Add(1)
				if err != nil || uint64(len(got)) != r.Size {
					failed.Add(1)
				}
			}
		}(g)
	}
	head := base
	for i := 0; i < 6; i++ {
		resp := f.edit(head, wr(fmt.Sprintf("a/new-%d", i), strings.Repeat("x", 1000*i+1)))
		head = restic.TestParseID(resp.Head.Snapshot)
		headsMu.Lock()
		heads = append(heads, head)
		headsMu.Unlock()
		// Read the new file of the new head by path.
		got, err := contentRead(ctx, socket, contentReadReq{Source: head, Path: []byte(fmt.Sprintf("a/new-%d", i)), FileSize: uint64(1000*i + 1), Length: uint64(1000*i + 1), Timeout: 5 * time.Second})
		rtest.OK(t, err)
		rtest.Equals(t, strings.Repeat("x", 1000*i+1), string(got))
	}
	close(stop)
	wg.Wait()
	t.Logf("%d content reads during 6 tree writes", reads.Load())
	rtest.Equals(t, int64(0), failed.Load())
	rtest.Assert(t, reads.Load() > 0, "no reads ran")
}
