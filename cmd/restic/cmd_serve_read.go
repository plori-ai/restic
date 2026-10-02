package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/restic/restic/internal/bloblru"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/walker"
	"github.com/spf13/cobra"
)

func newServeReadCommand(gopts *global.Options) *cobra.Command {
	var socket string
	cmd := &cobra.Command{Use: "serve-read --socket PATH", Short: "Serve exact snapshot reads over a private Unix socket", GroupID: cmdGroupAdvanced, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if socket == "" {
				return errors.New("--socket is required")
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			printer := ui.NewProgressPrinter(false, gopts.Verbosity, gopts.Term)
			ctx, repo, unlock, err := openWithReadLock(ctx, *gopts, gopts.NoLock, printer)
			if err != nil {
				return err
			}
			defer unlock()
			if err = repo.LoadIndex(ctx, printer); err != nil {
				return err
			}
			if err := serveReadListen(ctx, socket, newServeReadHandler(repo)); err != nil {
				return err
			}
			return ErrOK
		}}
	cmd.Flags().StringVar(&socket, "socket", "", "platform Unix socket `path` (mode 0600)")
	return cmd
}

// Bind inside a private directory, then publish the already chmod'ed socket.
// The parent directory must be controlled by the platform. Never remove an
// existing socket: it may belong to another live reader.
func serveReadSocket(socket string) (net.Listener, func(), error) {
	if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		return nil, nil, errors.New("socket path already exists or is inaccessible")
	}
	dir, err := os.MkdirTemp(filepath.Dir(socket), ".serve-read-")
	if err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(dir)
	temporary := filepath.Join(dir, "s")
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: temporary, Net: "unix"})
	if err != nil {
		return nil, nil, err
	}
	ln.SetUnlinkOnClose(false)
	if err = os.Chmod(temporary, 0600); err == nil {
		// Link publishes without replacing a concurrently created destination.
		err = os.Link(temporary, socket)
	}
	if err != nil {
		_ = ln.Close()
		return nil, nil, err
	}
	return ln, func() { _ = ln.Close(); _ = os.Remove(socket) }, nil
}

func serveReadListen(ctx context.Context, socket string, handler http.Handler) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ln, cleanup, err := serveReadSocket(socket)
	if err != nil {
		return err
	}
	defer cleanup()
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 5 * time.Minute, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
		BaseContext: func(net.Listener) context.Context { return ctx }, ErrorLog: log.New(io.Discard, "", 0)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if srv.Shutdown(shutdown) != nil {
			_ = srv.Close()
		}
	}()
	err = srv.Serve(ln)
	// Also release the shutdown goroutine when Serve fails independently.
	if !errors.Is(err, http.ErrServerClosed) {
		_ = srv.Close()
		cancel()
		<-done
		return err
	}
	<-done
	return nil
}

type serveReadHandler struct {
	repo  *repository.Repository
	cache *bloblru.Cache
	gate  chan struct{}
	roots map[restic.ID]restic.ID
}

func newServeReadHandler(repo *repository.Repository) *serveReadHandler {
	return &serveReadHandler{repo: repo, cache: bloblru.New(64 << 20), gate: make(chan struct{}, 1), roots: make(map[restic.ID]restic.ID)}
}

func (s *serveReadHandler) LoadBlob(ctx context.Context, typ restic.BlobType, id restic.ID, _ []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.cache.GetOrCompute(id, func() ([]byte, error) {
		blob, err := s.repo.LoadBlob(ctx, typ, id, nil)
		if err == nil || ctx.Err() != nil {
			return blob, err
		}
		// A --no-lock reader can retain pack locations removed by prune. The
		// upstream incremental loader resets its index when old indexes disappear
		// (internal/repository/index/master_index.go:prepareIncrementalLoad).
		// Repository errors are not typed, so retry once after any failed blob
		// load; authentication/hash validation still happens in LoadBlob.
		if err = s.repo.LoadIndex(ctx, nil); err != nil {
			return nil, err
		}
		return s.repo.LoadBlob(ctx, typ, id, nil)
	})
}

func (s *serveReadHandler) prepare(ctx context.Context, id restic.ID) (restic.ID, error) {
	if root, ok := s.roots[id]; ok {
		return root, nil
	}
	// Exactly one refresh for an uncached ID, serialized with every repository
	// read. This also discovers packs below an already-known root tree.
	if err := s.repo.LoadIndex(ctx, nil); err != nil {
		return restic.ID{}, err
	}
	sn, err := data.LoadSnapshot(ctx, s.repo, id)
	if err != nil {
		found := false
		listErr := s.repo.List(ctx, restic.SnapshotFile, func(candidate restic.ID, _ int64) error {
			if candidate == id {
				found = true
			}
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
	tree, err := data.LoadTree(ctx, s, *sn.Tree)
	if err != nil {
		return restic.ID{}, err
	}
	for item := range tree {
		if item.Error != nil {
			return restic.ID{}, item.Error
		}
		if ctx.Err() != nil {
			return restic.ID{}, ctx.Err()
		}
	}
	if len(s.roots) >= 128 {
		clear(s.roots)
	}
	s.roots[id] = *sn.Tree
	return *sn.Tree, nil
}

func serveReadError(w http.ResponseWriter, code int) { http.Error(w, http.StatusText(code), code) }

func (s *serveReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if len(r.RequestURI) > 8192 {
		serveReadError(w, 414)
		return
	}
	method := http.MethodGet
	switch r.URL.Path {
	case "/prepare":
		method = http.MethodPost
	case "/tree", "/walk", "/skeleton", "/file", "/snapshots":
	default:
		serveReadError(w, 404)
		return
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		serveReadError(w, 405)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	q := r.URL.Query()
	selector := q.Get("snapshot")
	if method == http.MethodPost {
		var body struct {
			Snapshot string `json:"snapshot"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if dec.Decode(&body) != nil {
			serveReadError(w, 400)
			return
		}
		var extra any
		if dec.Decode(&extra) != io.EOF {
			serveReadError(w, 400)
			return
		}
		selector = body.Snapshot
	} else if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		serveReadError(w, 400)
		return
	}
	var id restic.ID
	var err error
	if r.URL.Path != "/snapshots" {
		id, err = restic.ParseID(selector)
		if err != nil {
			serveReadError(w, 400)
			return
		}
	}
	p := q.Get("path")
	if r.URL.Path == "/tree" || r.URL.Path == "/file" {
		if !strings.HasPrefix(p, "/") || path.Clean(p) != p || strings.ContainsRune(p, 0) {
			serveReadError(w, 400)
			return
		}
	}
	// Context-aware serialization avoids unsafe index replacement and permits
	// canceled requests to leave the queue without waiting for a long walk.
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-r.Context().Done():
		return
	}
	if r.Context().Err() != nil {
		return
	}
	if r.URL.Path == "/snapshots" {
		snapshots := []Snapshot{}
		err = data.ForAllSnapshots(r.Context(), s.repo, s.repo, nil, func(_ restic.ID, sn *data.Snapshot, err error) error {
			if err != nil {
				return err
			}
			snapshots = append(snapshots, Snapshot{Snapshot: sn, ID: sn.ID(), ShortID: sn.ID().Str()})
			return nil
		})
		if err != nil {
			serveReadError(w, 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshots)
		return
	}
	root, err := s.prepare(r.Context(), id)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, os.ErrNotExist) {
			code = http.StatusNotFound
		}
		serveReadError(w, code)
		return
	}
	switch r.URL.Path {
	case "/prepare":
		w.WriteHeader(http.StatusNoContent)
	case "/walk":
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Trailer", "X-Restic-Error")
		enc := json.NewEncoder(w)
		err = walker.Walk(r.Context(), s, root, walker.WalkVisitor{ProcessNode: func(_ restic.ID, p string, node *data.Node, err error) error {
			if err != nil {
				return err
			}
			if node == nil {
				return nil
			}
			return enc.Encode(struct {
				Path string     `json:"path"`
				Node *data.Node `json:"node"`
			}{p, node})
		}})
		if err != nil {
			w.Header().Set("X-Restic-Error", "read failed")
		}
	case "/skeleton":
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Trailer", "X-Restic-Error")
		if err := s.skeleton(r.Context(), w, root); err != nil {
			w.Header().Set("X-Restic-Error", "read failed")
		}
	case "/tree", "/file":
		dir := p
		if r.URL.Path == "/file" {
			dir = path.Dir(p)
		}
		treeID, err := data.FindTreeDirectory(r.Context(), s, &root, dir)
		if err != nil {
			serveReadError(w, 404)
			return
		}
		tree, err := data.LoadTree(r.Context(), s, *treeID)
		if err != nil {
			serveReadError(w, 500)
			return
		}
		nodes := []*data.Node{}
		for item := range tree {
			if item.Error != nil {
				serveReadError(w, 500)
				return
			}
			if r.Context().Err() != nil {
				return
			}
			if r.URL.Path == "/file" {
				if item.Node.Name == path.Base(p) {
					s.file(w, r, item.Node)
					return
				}
			} else {
				nodes = append(nodes, item.Node)
			}
		}
		if r.URL.Path == "/file" {
			serveReadError(w, 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(nodes)
	}
}

func (s *serveReadHandler) file(w http.ResponseWriter, r *http.Request, node *data.Node) {
	if node.Type != data.NodeTypeFile {
		serveReadError(w, 400)
		return
	}
	q := r.URL.Query()
	offset, length := uint64(0), node.Size
	for key, dst := range map[string]*uint64{"offset": &offset, "length": &length} {
		if q.Has(key) {
			n, err := strconv.ParseUint(q.Get(key), 10, 64)
			if err != nil {
				serveReadError(w, 400)
				return
			}
			*dst = n
		}
	}
	if offset > node.Size {
		serveReadError(w, 416)
		return
	}
	length = min(length, node.Size-offset)
	sizes := make([]uint64, len(node.Content))
	var total uint64
	for i, id := range node.Content {
		if r.Context().Err() != nil {
			return
		}
		n, ok := s.repo.LookupBlobSize(restic.DataBlob, id)
		if !ok || uint64(n) > ^uint64(0)-total {
			serveReadError(w, 500)
			return
		}
		sizes[i] = uint64(n)
		total += uint64(n)
	}
	if total != node.Size {
		serveReadError(w, 500)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatUint(length, 10))
	// Query ranges return 200 with the requested length, including zero at EOF.
	for i, id := range node.Content {
		if length == 0 {
			break
		}
		if offset >= sizes[i] {
			offset -= sizes[i]
			continue
		}
		blob, err := s.LoadBlob(r.Context(), restic.DataBlob, id, nil)
		if err != nil || uint64(len(blob)) != sizes[i] {
			panic(http.ErrAbortHandler)
		}
		n := min(length, uint64(len(blob))-offset)
		if _, err = w.Write(blob[offset : offset+n]); err != nil {
			return
		}
		if err = http.NewResponseController(w).Flush(); err != nil {
			return
		}
		length -= n
		offset = 0
	}
}
