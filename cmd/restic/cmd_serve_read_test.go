package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func serveReadFixture(t *testing.T, repo *repository.Repository, payload string) (restic.ID, map[string]*data.Node) {
	t.Helper()
	ctx := context.Background()
	stamp := time.Unix(1234567890, 123456789).UTC()
	nodes := map[string]*data.Node{}
	var root restic.ID
	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) error {
		var content restic.IDs
		for _, part := range []string{payload[:len(payload)/2], payload[len(payload)/2:]} {
			id, _, _, err := up.SaveBlob(ctx, restic.DataBlob, []byte(part), restic.ID{}, false)
			if err != nil {
				return err
			}
			content = append(content, id)
		}
		nodes["/dir/file"] = &data.Node{Name: "file", Type: data.NodeTypeFile, Mode: 0640, Size: uint64(len(payload)), Inode: 123, DeviceID: 456, Links: 2, ModTime: stamp, ChangeTime: stamp, Content: content}
		alias := *nodes["/dir/file"]
		alias.Name = "hardlink"
		nodes["/dir/hardlink"] = &alias
		nodes["/dir/symlink"] = &data.Node{Name: "symlink", Type: data.NodeTypeSymlink, Mode: os.ModeSymlink | 0777, LinkTarget: "file", ChangeTime: stamp}
		save := func(ns ...*data.Node) (restic.ID, error) {
			return data.SaveTree(ctx, up, func(yield func(data.NodeOrError) bool) {
				for _, n := range ns {
					if !yield(data.NodeOrError{Node: n}) {
						return
					}
				}
			})
		}
		sub, err := save(nodes["/dir/file"], nodes["/dir/hardlink"], nodes["/dir/symlink"])
		if err != nil {
			return err
		}
		nodes["/dir"] = &data.Node{Name: "dir", Type: data.NodeTypeDir, Mode: os.ModeDir | 0750, Subtree: &sub, ChangeTime: stamp}
		root, err = save(nodes["/dir"])
		return err
	}))
	id, err := data.SaveSnapshot(ctx, repo, &data.Snapshot{Tree: &root, Time: stamp})
	rtest.OK(t, err)
	return id, nodes
}

func serveReadRequest(h http.Handler, method, uri, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, uri, strings.NewReader(body)))
	return w
}

func TestServeReadMetadataAndRanges(t *testing.T) {
	writer, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{})
	id, original := serveReadFixture(t, writer, "abcdefghijklmno")
	reader := repository.TestOpenBackend(t, be)
	rtest.OK(t, reader.LoadIndex(context.Background(), nil))
	h := newServeReadHandler(reader)
	for i := 0; i < 2; i++ {
		w := serveReadRequest(h, "POST", "/prepare", `{"snapshot":"`+id.String()+`"}`)
		rtest.Equals(t, 204, w.Code)
	}
	w := serveReadRequest(h, "GET", "/tree?snapshot="+id.String()+"&path=/dir", "")
	rtest.Equals(t, 200, w.Code)
	var nodes []*data.Node
	rtest.OK(t, json.Unmarshal(w.Body.Bytes(), &nodes))
	rtest.Equals(t, 3, len(nodes))
	for _, node := range nodes {
		rtest.Equals(t, original["/dir/"+node.Name], node)
	}
	w = serveReadRequest(h, "GET", "/walk?snapshot="+id.String(), "")
	rtest.Equals(t, 200, w.Code)
	found := map[string]*data.Node{}
	dec := json.NewDecoder(w.Body)
	for {
		var entry struct {
			Path string
			Node *data.Node
		}
		err := dec.Decode(&entry)
		if err == io.EOF {
			break
		}
		rtest.OK(t, err)
		found[entry.Path] = entry.Node
	}
	// Equality includes fields find --json exports, plus ordered content IDs,
	// subtree and attributes that find intentionally suppresses.
	rtest.Equals(t, original, found)
	for _, tc := range []struct {
		query, want string
		code        int
	}{
		{"", "abcdefghijklmno", 200}, {"&offset=5&length=6", "fghijk", 200},
		{"&offset=13&length=99", "no", 200}, {"&offset=15", "", 200},
		{"&length=0", "", 200}, {"&offset=16", "", 416},
		{"&offset=-1", "", 400}, {"&length=18446744073709551616", "", 400},
	} {
		w := serveReadRequest(h, "GET", "/file?snapshot="+id.String()+"&path=/dir/file"+tc.query, "")
		rtest.Equals(t, tc.code, w.Code)
		if tc.code == 200 {
			rtest.Equals(t, tc.want, w.Body.String())
		}
	}
}

func TestServeReadFreshSnapshot(t *testing.T) {
	writer, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{})
	reader := repository.TestOpenBackend(t, be)
	rtest.OK(t, reader.LoadIndex(context.Background(), nil))
	h := newServeReadHandler(reader)
	w := serveReadRequest(h, "GET", "/snapshots", "")
	rtest.Equals(t, "[]\n", w.Body.String())
	id, nodes := serveReadFixture(t, writer, "new snapshot packs")
	_, known := reader.LookupBlobSize(restic.DataBlob, nodes["/dir/file"].Content[0])
	rtest.Assert(t, !known, "reader must start with a stale index")
	w = serveReadRequest(h, "GET", "/file?snapshot="+id.String()+"&path=/dir/file", "")
	rtest.Equals(t, 200, w.Code)
	rtest.Equals(t, "new snapshot packs", w.Body.String())
	w = serveReadRequest(h, "GET", "/snapshots", "")
	var snapshots []Snapshot
	rtest.OK(t, json.Unmarshal(w.Body.Bytes(), &snapshots))
	rtest.Equals(t, 1, len(snapshots))
	rtest.Equals(t, id, *snapshots[0].ID)
	// Listing must remain fresh even when a second snapshot arrives immediately.
	second, _ := serveReadFixture(t, writer, "another snapshot")
	w = serveReadRequest(h, "POST", "/prepare", `{"snapshot":"`+second.String()+`"}`)
	rtest.Equals(t, 204, w.Code)
	w = serveReadRequest(h, "GET", "/snapshots", "")
	rtest.OK(t, json.Unmarshal(w.Body.Bytes(), &snapshots))
	rtest.Equals(t, 2, len(snapshots))
	w = serveReadRequest(h, "POST", "/prepare", `{"snapshot":"`+strings.Repeat("0", 64)+`"}`)
	rtest.Equals(t, 404, w.Code)
}

func TestServeReadValidationAndCancellation(t *testing.T) {
	h := newServeReadHandler(repository.TestRepository(t))
	for _, tc := range []struct {
		method, uri, body string
		code              int
	}{
		{"POST", "/prepare", `{"snapshot":"latest"}`, 400},
		{"POST", "/prepare", `{"snapshot":"x","other":1}`, 400},
		{"POST", "/prepare", strings.Repeat(" ", 4097), 400},
		{"POST", "/prepare", `{}` + `{}`, 400},
		{"GET", "/snapshots", "unexpected body", 400},
		{"DELETE", "/snapshots", "", 405}, {"GET", "/missing", "", 404},
		{"GET", "/tree?snapshot=" + strings.Repeat("0", 64) + "&path=/../dir", "", 400},
		{"GET", "/" + strings.Repeat("x", 8192), "", 414},
	} {
		rtest.Equals(t, tc.code, serveReadRequest(h, tc.method, tc.uri, tc.body).Code)
	}
	h.gate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/snapshots", nil).WithContext(ctx))
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled request stuck behind gate")
	}
	<-h.gate
	_, err := h.LoadBlob(ctx, restic.DataBlob, restic.ID{}, nil)
	rtest.Equals(t, context.Canceled, err)
}

func TestServeReadSocketAndShutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix sockets require Unix")
	}
	// Keep the socket address below the Unix address length limit.
	dir, err := os.MkdirTemp("", "rfork-")
	rtest.OK(t, err)
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "read.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serveReadListen(ctx, socket, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err = os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stat, err := os.Stat(socket)
	rtest.OK(t, err)
	rtest.Equals(t, os.FileMode(0600), stat.Mode().Perm())
	ln, _, err := serveReadSocket(socket)
	rtest.Assert(t, err != nil && ln == nil, "must refuse existing socket")
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	resp, err := client.Get("http://unix/")
	rtest.OK(t, err)
	_ = resp.Body.Close()
	rtest.Equals(t, 204, resp.StatusCode)
	cancel()
	select {
	case err = <-done:
		rtest.OK(t, err)
	case <-time.After(6 * time.Second):
		t.Fatal("shutdown stuck")
	}
	_, err = os.Stat(socket)
	rtest.Assert(t, os.IsNotExist(err), "socket must be removed")
	rtest.OK(t, os.WriteFile(socket, []byte("keep"), 0600))
	_, _, err = serveReadSocket(socket)
	rtest.Assert(t, err != nil, "must refuse regular file")
	b, err := os.ReadFile(socket)
	rtest.OK(t, err)
	rtest.Assert(t, reflect.DeepEqual(b, []byte("keep")), "existing file changed")
}

func TestServeReadWalkFailureTrailer(t *testing.T) {
	repo := repository.TestRepository(t)
	ctx := context.Background()
	missing := restic.Hash([]byte("missing tree"))
	var root restic.ID
	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) error {
		var err error
		root, err = data.SaveTree(ctx, up, func(yield func(data.NodeOrError) bool) {
			yield(data.NodeOrError{Node: &data.Node{Name: "broken", Type: data.NodeTypeDir, Subtree: &missing}})
		})
		return err
	}))
	id, err := data.SaveSnapshot(ctx, repo, &data.Snapshot{Tree: &root})
	rtest.OK(t, err)
	w := serveReadRequest(newServeReadHandler(repo), "GET", "/walk?snapshot="+id.String(), "")
	resp := w.Result()
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	rtest.OK(t, err)
	rtest.Equals(t, "read failed", resp.Trailer.Get("X-Restic-Error"))
}

// Count backend index listings to verify refresh is bounded per request.
type serveReadCountingBackend struct {
	backend.Backend
	indexes atomic.Int32
}

func (b *serveReadCountingBackend) List(ctx context.Context, typ backend.FileType, fn func(backend.FileInfo) error) error {
	if typ == backend.IndexFile {
		b.indexes.Add(1)
	}
	return b.Backend.List(ctx, typ, fn)
}
func TestServeReadRefreshBound(t *testing.T) {
	writer, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{})
	id, _ := serveReadFixture(t, writer, "bounded refresh")
	counted := &serveReadCountingBackend{Backend: be}
	reader := repository.TestOpenBackend(t, counted)
	rtest.OK(t, reader.LoadIndex(context.Background(), nil))
	h := newServeReadHandler(reader)
	before := counted.indexes.Load()
	body := `{"snapshot":"` + id.String() + `"}`
	rtest.Equals(t, 204, serveReadRequest(h, "POST", "/prepare", body).Code)
	rtest.Equals(t, before+1, counted.indexes.Load())
	rtest.Equals(t, 204, serveReadRequest(h, "POST", "/prepare", body).Code)
	rtest.Equals(t, before+1, counted.indexes.Load())
	body = `{"snapshot":"` + strings.Repeat("0", 64) + `"}`
	rtest.Equals(t, 404, serveReadRequest(h, "POST", "/prepare", body).Code)
	rtest.Equals(t, before+2, counted.indexes.Load())
}

// A retained snapshot must remain readable after prune has repacked all blobs and
// removed the old packs/indexes, even if prepare has cached its root already.
func TestServeReadReloadsAfterRepack(t *testing.T) {
	ctx := context.Background()
	writer, be := repository.TestRepositoryWithBackend(t, nil, 0, repository.Options{})
	id, _ := serveReadFixture(t, writer, "retained content after repack")
	reader := repository.TestOpenBackend(t, be)
	rtest.OK(t, reader.LoadIndex(ctx, nil))
	h := newServeReadHandler(reader)
	rtest.Equals(t, 204, serveReadRequest(h, "POST", "/prepare", `{"snapshot":"`+id.String()+`"}`).Code)
	var obsolete []backend.Handle
	for _, typ := range []backend.FileType{backend.PackFile, backend.IndexFile} {
		rtest.OK(t, be.List(ctx, typ, func(fi backend.FileInfo) error {
			obsolete = append(obsolete, backend.Handle{Type: typ, Name: fi.Name})
			return nil
		}))
	}
	blobs := map[restic.BlobHandle][]byte{}
	rtest.OK(t, writer.ListBlobs(ctx, func(pb restic.PackedBlob) {
		b, err := writer.LoadBlob(ctx, pb.Type, pb.ID, nil)
		rtest.OK(t, err)
		blobs[pb.BlobHandle] = b
	}))
	rtest.OK(t, writer.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) error {
		for bh, b := range blobs {
			if _, _, _, err := up.SaveBlob(ctx, bh.Type, b, bh.ID, true); err != nil {
				return err
			}
		}
		return nil
	}))
	for _, handle := range obsolete {
		rtest.OK(t, be.Remove(ctx, handle))
	}
	w := serveReadRequest(h, "GET", "/file?snapshot="+id.String()+"&path=/dir/file", "")
	rtest.Equals(t, 200, w.Code)
	rtest.Equals(t, "retained content after repack", w.Body.String())
	// Metadata blobs also use the same retry path (discard the immutable cache).
	h.cache = newServeReadHandler(reader).cache
	rtest.Equals(t, 200, serveReadRequest(h, "GET", "/walk?snapshot="+id.String(), "").Code)
}
