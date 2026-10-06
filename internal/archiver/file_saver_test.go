package archiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/restic/chunker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/test"
	"golang.org/x/sync/errgroup"
)

func createTestFiles(t testing.TB, num int) (files []string) {
	tempdir := test.TempDir(t)

	for i := 0; i < num; i++ {
		filename := fmt.Sprintf("testfile-%d", i)
		err := os.WriteFile(filepath.Join(tempdir, filename), []byte(filename), 0600)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, filepath.Join(tempdir, filename))
	}

	return files
}

type hintedFile struct {
	fs.File
	io.Reader
	size     uint64
	closeErr error
}

func (f hintedFile) Read(p []byte) (int, error) { return f.Reader.Read(p) }
func (f hintedFile) Close() error               { return f.closeErr }
func (f hintedFile) ToNode(bool, func(string, ...any)) (*data.Node, error) {
	return &data.Node{Name: "file", Type: data.NodeTypeFile, Size: f.size}, nil
}

type terminalReader struct{ err error }

func (r terminalReader) Read([]byte) (int, error) { return 0, r.err }

type shortReader struct{ io.Reader }

func (r shortReader) Read(p []byte) (int, error) {
	if len(p) > 97 {
		p = p[:97]
	}
	return r.Reader.Read(p)
}

func TestFileSaverSmallFileBoundaries(t *testing.T) {
	pol, err := chunker.RandomPolynomial()
	test.OK(t, err)
	ctx := context.Background()
	payload := make([]byte, 2*chunker.MinSize+1234)
	_, err = rand.New(rand.NewSource(1)).Read(payload)
	test.OK(t, err)
	// Reuse one chunker and pool across cases, including transitions between
	// the direct path, ordinary chunking, and a grown/synthetic-size file.
	chnker := chunker.New(nil, pol)
	saver := &mockSaver{saved: make(map[string]int)}
	s := &fileSaver{pol: pol, uploader: saver, saveFilePool: newBufferPool(chunker.MaxSize), CompleteBlob: func(uint64) {}}
	s.NodeFromFileInfo = func(_, _ string, meta ToNoder, ignore bool) (*data.Node, error) {
		return meta.ToNode(ignore, t.Logf)
	}
	for _, size := range []int{0, 1, 1800, chunker.MinSize - 65, chunker.MinSize - 1, chunker.MinSize, chunker.MinSize + 1, len(payload)} {
		for _, hint := range []uint64{0, uint64(size), uint64(len(payload))} {
			t.Run(fmt.Sprintf("size=%d/hint=%d", size, hint), func(t *testing.T) {
				content := payload[:size]
				var want restic.IDs
				original := chunker.New(bytes.NewReader(content), pol)
				for {
					chunk, err := original.Next(nil)
					if err == io.EOF {
						break
					}
					test.OK(t, err)
					want = append(want, restic.Hash(chunk.Data))
				}
				done := make(chan futureNodeResult, 1)
				readComplete := false
				s.saveFile(ctx, chnker, "/file", "file", hintedFile{Reader: shortReader{bytes.NewReader(content)}, size: hint}, func() {}, func() { readComplete = true }, func(res futureNodeResult) { done <- res })
				res := <-done
				test.OK(t, res.err)
				test.Assert(t, readComplete, "completion before read finished")
				test.Equals(t, uint64(size), res.node.Size)
				// Empty files must retain [] instead of null in tree JSON.
				test.Assert(t, res.node.Content != nil, "nil content")
				test.Equals(t, len(want), len(res.node.Content))
				for i := range want {
					test.Equals(t, want[i], res.node.Content[i])
				}
			})
		}
	}
}

func TestFileSaverSmallFileErrors(t *testing.T) {
	pol, err := chunker.RandomPolynomial()
	test.OK(t, err)
	failure := errors.New("read or close failure")
	for _, file := range []hintedFile{
		{Reader: terminalReader{failure}, size: 0},
		{Reader: io.MultiReader(bytes.NewReader([]byte("partial")), terminalReader{failure}), size: 1},
		{Reader: io.MultiReader(bytes.NewReader(make([]byte, chunker.MinSize)), terminalReader{failure}), size: 0},
		{Reader: bytes.NewReader([]byte("content")), size: 1, closeErr: failure},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		s, _, ctx, wg := startFileSaver(ctx, t, fs.Local{})
		completeReading := false
		done := make(chan futureNodeResult, 1)
		s.saveFile(ctx, chunker.New(nil, pol), "/file", "file", file, func() {}, func() { completeReading = true }, func(res futureNodeResult) { done <- res })
		res := <-done
		test.Assert(t, errors.Is(res.err, failure), "error lost: %v", res.err)
		test.Assert(t, res.node == nil && !completeReading, "failed file reported success")
		s.TriggerShutdown()
		test.OK(t, wg.Wait())
		cancel()
	}
}

func TestFileSaverSmallFileInvalidPolynomial(t *testing.T) {
	for _, pol := range []chunker.Pol{0, 1 << 7, 1 << 54} {
		t.Run(pol.String(), func(t *testing.T) {
			content := []byte("small file")
			original := chunker.New(bytes.NewReader(content), pol)
			_, want := original.Next(nil)
			test.Assert(t, want != nil, "test requires an invalid polynomial")
			ctx := context.Background()
			s := &fileSaver{pol: pol, uploader: &mockSaver{saved: make(map[string]int)}, saveFilePool: newBufferPool(chunker.MaxSize), CompleteBlob: func(uint64) {}}
			s.NodeFromFileInfo = func(_, _ string, meta ToNoder, ignore bool) (*data.Node, error) { return meta.ToNode(ignore, t.Logf) }
			done := make(chan futureNodeResult, 1)
			s.saveFile(ctx, chunker.New(nil, pol), "/file", "file", hintedFile{Reader: bytes.NewReader(content), size: uint64(len(content))}, func() {}, func() { t.Error("invalid polynomial completed reading") }, func(res futureNodeResult) { done <- res })
			res := <-done
			test.Assert(t, res.err != nil && strings.Contains(res.err.Error(), want.Error()), "chunker validation lost: want %v, got %v", want, res.err)
			test.Assert(t, res.node == nil, "invalid polynomial returned a node")
		})
	}
}

func startFileSaver(ctx context.Context, t testing.TB, _ fs.FS) (*fileSaver, *mockSaver, context.Context, *errgroup.Group) {
	wg, ctx := errgroup.WithContext(ctx)

	workers := uint(runtime.NumCPU())
	pol, err := chunker.RandomPolynomial()
	if err != nil {
		t.Fatal(err)
	}

	saver := &mockSaver{saved: make(map[string]int)}
	s := newFileSaver(ctx, wg, saver, pol, workers)
	s.NodeFromFileInfo = func(snPath, filename string, meta ToNoder, ignoreXattrListError bool) (*data.Node, error) {
		return meta.ToNode(ignoreXattrListError, t.Logf)
	}

	return s, saver, ctx, wg
}

func TestFileSaver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startFn := func() {}
	completeReadingFn := func() {}
	completeFn := func(*data.Node, ItemStats) {}

	files := createTestFiles(t, 15)
	testFs := fs.Local{}
	s, saver, ctx, wg := startFileSaver(ctx, t, testFs)

	var results []futureNode

	for _, filename := range files {
		f, err := testFs.OpenFile(filename, os.O_RDONLY, false)
		if err != nil {
			t.Fatal(err)
		}

		ff := s.Save(ctx, filename, filename, f, startFn, completeReadingFn, completeFn)
		results = append(results, ff)
	}

	for _, file := range results {
		fnr := file.take(ctx)
		if fnr.err != nil {
			t.Errorf("unable to save file: %v", fnr.err)
		}
	}

	test.Assert(t, len(saver.saved) == len(files), "expected %d saved files, got %d", len(files), len(saver.saved))

	s.TriggerShutdown()

	err := wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
}
