package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
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

// lzfmTestRecord is a decoded metadata-stream record.
type lzfmTestRecord struct {
	Path       []byte
	Type       uint64
	Mode       uint64
	UID, GID   uint64
	Mtime      int64
	Atime      int64
	Size       uint64
	DevMajor   uint64
	DevMinor   uint64
	LinkTarget []byte
	LinkGroup  uint64
	LinkCount  uint64
	Xattrs     [][2][]byte
	Blobs      []lzfmTestBlob
}

type lzfmTestBlob struct {
	ID     []byte
	Length uint64
}

type lzfmTestStream struct {
	Source, Root []byte
	Producer     string
	Records      []lzfmTestRecord
}

// lzfmDecode decodes and verifies a complete metadata stream. The default is
// the test decoder below, written from doc/plori-lazy-fill.md; building with
// the tag lazyfill_xcheck replaces it with the lazyfill protocol package's
// decoder (serve_read_lazyfill_xcheck_test.go).
var lzfmDecode = lzfmTestDecode

// contentRead reads [off, off+length) of a file through POST /v1/read on a
// Unix socket, verifying the length and the status trailer. The default is
// testContentRead; lazyfill_xcheck replaces it with lazyfill's HTTP client.
var contentRead = testContentRead

// contentReadReq is the input of contentRead.
type contentReadReq struct {
	Source   restic.ID
	Path     []byte
	FileSize uint64
	Blobs    []lzfmTestBlob
	Offset   uint64
	Length   uint64
	Timeout  time.Duration
}

// contentReadError is a classified read failure (the binding's code).
type contentReadError struct{ Code, Message string }

func (e *contentReadError) Error() string { return e.Code + ": " + e.Message }

func lzfmTestDecode(r io.Reader) (*lzfmTestStream, error) {
	br := bufio.NewReader(r)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(br, magic); err != nil || string(magic) != "LZFM" {
		return nil, fmt.Errorf("magic %q: %v", magic, err)
	}
	if v, err := binary.ReadUvarint(br); err != nil || v != 1 {
		return nil, fmt.Errorf("version %d: %v", v, err)
	}
	h := sha256.New()
	next := func() (byte, []byte, error) {
		kind, err := br.ReadByte()
		if err != nil {
			return 0, nil, fmt.Errorf("incomplete: %w", err)
		}
		n, err := binary.ReadUvarint(br)
		if err != nil || n > 16<<20 {
			return 0, nil, fmt.Errorf("frame length %d: %v", n, err)
		}
		p := make([]byte, n)
		if _, err := io.ReadFull(br, p); err != nil {
			return 0, nil, err
		}
		if kind == 'H' || kind == 'R' {
			h.Write(binary.AppendUvarint([]byte{kind}, n))
			h.Write(p)
		}
		return kind, p, nil
	}
	type field struct {
		tag uint64
		u   uint64
		b   []byte
	}
	// Field types per tag: 'u' uvarint, 's' varint, 'b' bytes.
	fields := func(p []byte, types string, repeated ...uint64) ([]field, error) {
		var out []field
		var last uint64
		for len(p) > 0 {
			t, n := binary.Uvarint(p)
			if n <= 0 || t == 0 || int(t) > len(types) {
				return nil, fmt.Errorf("tag %d", t)
			}
			p = p[n:]
			if t < last || (t == last && !slices.Contains(repeated, t)) {
				return nil, fmt.Errorf("tag %d after %d", t, last)
			}
			last = t
			f := field{tag: t}
			switch types[t-1] {
			case 'u':
				f.u, n = binary.Uvarint(p)
			case 's':
				var v int64
				v, n = binary.Varint(p)
				f.u = uint64(v)
			case 'b':
				var l uint64
				l, n = binary.Uvarint(p)
				if n > 0 && l <= uint64(len(p)-n) {
					f.b = p[n : n+int(l)]
					n += int(l)
				} else {
					n = 0
				}
			}
			if n <= 0 {
				return nil, fmt.Errorf("value of tag %d", t)
			}
			p = p[n:]
			out = append(out, f)
		}
		return out, nil
	}
	// two splits a composite value: a length-prefixed byte string followed
	// by a uvarint and the rest.
	two := func(b []byte) ([]byte, uint64, []byte, error) {
		l, n := binary.Uvarint(b)
		if n <= 0 || l > uint64(len(b)-n) {
			return nil, 0, nil, errors.New("composite value")
		}
		a, rest := b[n:n+int(l)], b[n+int(l):]
		v, m := binary.Uvarint(rest)
		if m <= 0 {
			return nil, 0, nil, errors.New("composite value")
		}
		return a, v, rest[m:], nil
	}
	kind, p, err := next()
	if err != nil || kind != 'H' {
		return nil, fmt.Errorf("header frame %q: %v", kind, err)
	}
	fs, err := fields(p, "bbb")
	if err != nil {
		return nil, err
	}
	st := &lzfmTestStream{}
	for _, f := range fs {
		switch f.tag {
		case 1:
			st.Source = f.b
		case 2:
			st.Root = f.b
		case 3:
			st.Producer = string(f.b)
		}
	}
	var counts [6]uint64
	for {
		kind, p, err := next()
		if err != nil {
			return nil, err
		}
		switch kind {
		case 'R':
			// Tags 6 and 7 are signed; tags 14 and 15 repeat.
			fs, err := fields(p, "buuuussuuubuubb", 14, 15)
			if err != nil {
				return nil, err
			}
			var r lzfmTestRecord
			for _, f := range fs {
				switch f.tag {
				case 1:
					r.Path = f.b
				case 2:
					r.Type = f.u
				case 3:
					r.Mode = f.u
				case 4:
					r.UID = f.u
				case 5:
					r.GID = f.u
				case 6:
					r.Mtime = int64(f.u)
				case 7:
					r.Atime = int64(f.u)
				case 8:
					r.Size = f.u
				case 9:
					r.DevMajor = f.u
				case 10:
					r.DevMinor = f.u
				case 11:
					r.LinkTarget = f.b
				case 12:
					r.LinkGroup = f.u
				case 13:
					r.LinkCount = f.u
				case 14:
					name, l, value, err := two(f.b)
					if err != nil || uint64(len(value)) != l {
						return nil, fmt.Errorf("xattr field: %v", err)
					}
					r.Xattrs = append(r.Xattrs, [2][]byte{name, value})
				case 15:
					id, length, rest, err := two(f.b)
					if err != nil || len(rest) != 0 || length == 0 {
						return nil, fmt.Errorf("blob field: %v", err)
					}
					r.Blobs = append(r.Blobs, lzfmTestBlob{ID: id, Length: length})
				}
			}
			counts[0]++
			switch r.Type {
			case lzfmDir:
				counts[1]++
			case lzfmFile:
				counts[2]++
				counts[5] += r.Size
			case lzfmSymlink:
				counts[3]++
			default:
				counts[4]++
			}
			st.Records = append(st.Records, r)
		case 'T':
			fs, err := fields(p, "uuuuuubb")
			if err != nil {
				return nil, err
			}
			var got [6]uint64
			var digest []byte
			for _, f := range fs {
				switch f.tag {
				case 8:
					return nil, fmt.Errorf("producer failure: %s", f.b)
				case 7:
					digest = f.b
				default:
					got[f.tag-1] = f.u
				}
			}
			if got != counts || !bytes.Equal(digest, h.Sum(nil)) {
				return nil, fmt.Errorf("trailer %v digest %x, read %v %x", got, digest, counts, h.Sum(nil))
			}
			if _, err := br.ReadByte(); err != io.EOF {
				return nil, errors.New("data after trailer")
			}
			return st, nil
		default:
			return nil, fmt.Errorf("frame kind %q", kind)
		}
	}
}

// unixHTTPClient talks HTTP to a Unix socket.
func unixHTTPClient(socket string) *http.Client {
	return &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 128, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}}}
}

var testContentClients = map[string]*http.Client{}

func testContentRead(ctx context.Context, socket string, req contentReadReq) ([]byte, error) {
	cl, ok := testContentClients[socket]
	if !ok {
		panic("call testContentClient first")
	}
	body := contentRequest{Source: req.Source.String(), FileSize: req.FileSize, Offset: req.Offset, Length: req.Length}
	if len(req.Path) > 0 {
		body.Path = base64.StdEncoding.EncodeToString(req.Path)
	}
	pos, end := uint64(0), req.Offset+req.Length
	for _, b := range req.Blobs {
		bEnd := pos + b.Length
		if bEnd > req.Offset && pos < end {
			from, to := max(req.Offset, pos), min(end, bEnd)
			body.Spans = append(body.Spans, contentSpan{Blob: fmt.Sprintf("%x", b.ID), Offset: from - pos, Length: to - from})
		}
		pos = bEnd
	}
	js, _ := json.Marshal(body)
	hr, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://serve-read"+contentReadPath, bytes.NewReader(js))
	hr.Header.Set(contentVersionHeader, contentWireVersion)
	if req.Timeout > 0 {
		hr.Header.Set(contentTimeoutHeader, strconv.FormatInt(req.Timeout.Milliseconds(), 10))
	}
	resp, err := cl.Do(hr)
	if err != nil {
		return nil, &contentReadError{Code: "unavailable", Message: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		var e struct{ Code, Message string }
		_ = json.NewDecoder(resp.Body).Decode(&e)
		return nil, &contentReadError{Code: e.Code, Message: e.Message}
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &contentReadError{Code: "unavailable", Message: err.Error()}
	}
	if st := resp.Trailer.Get(contentStatusTrailer); st != "ok" {
		code, msg, _ := strings.Cut(strings.TrimPrefix(st, "error "), " ")
		return nil, &contentReadError{Code: code, Message: msg}
	}
	if uint64(len(got)) != req.Length {
		return nil, &contentReadError{Code: "corrupt", Message: fmt.Sprintf("%d of %d bytes", len(got), req.Length)}
	}
	return got, nil
}

// serveOnSocket serves h on a fresh Unix socket until the test ends.
func serveOnSocket(t testing.TB, h http.Handler) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "lzf")
	rtest.OK(t, err)
	socket := filepath.Join(dir, "s")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveReadListen(ctx, socket, h) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket did not appear")
		}
		time.Sleep(5 * time.Millisecond)
	}
	testContentClients[socket] = unixHTTPClient(socket)
	t.Cleanup(func() {
		cancel()
		rtest.OK(t, <-done)
		delete(testContentClients, socket)
		_ = os.RemoveAll(dir)
	})
	return socket
}

// fetchSkeleton reads /skeleton of a snapshot over the socket.
func fetchSkeleton(t testing.TB, socket string, id restic.ID) ([]byte, time.Duration) {
	t.Helper()
	start := time.Now()
	resp, err := unixHTTPClient(socket).Get("http://serve-read/skeleton?snapshot=" + id.String())
	rtest.OK(t, err)
	defer func() { _ = resp.Body.Close() }()
	rtest.Equals(t, http.StatusOK, resp.StatusCode)
	b, err := io.ReadAll(resp.Body)
	rtest.OK(t, err)
	return b, time.Since(start)
}

// genTreeSpec describes a generated snapshot: top directories, each with
// mid directories, each with leaf entries.
type genTreeSpec struct {
	top, mid, leaf int
	seed           int64
	// blobs is the number of distinct content blobs, of 1 B to maxBlob.
	blobs, maxBlob int
}

// genTree writes a snapshot of generated nodes with the repository API: plain
// files of zero to three blobs, empty files, symbolic links (some with
// non-UTF-8 targets), hard-link groups, extended attributes, non-UTF-8 names,
// FIFOs, sockets, character and block devices, setuid/setgid/sticky modes,
// and nanosecond, pre-1970 and zero timestamps. It returns the snapshot ID and
// the content of every blob.
func genTree(t testing.TB, repo *repository.Repository, spec genTreeSpec) (restic.ID, map[restic.ID][]byte) {
	t.Helper()
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(spec.seed))
	blobs := map[restic.ID][]byte{}
	var pool restic.IDs
	var root restic.ID
	rtest.OK(t, repo.WithBlobUploader(ctx, func(ctx context.Context, up restic.BlobSaverWithAsync) error {
		for len(pool) < spec.blobs {
			b := make([]byte, 1+rnd.Intn(spec.maxBlob))
			_, _ = rnd.Read(b)
			id, _, _, err := up.SaveBlob(ctx, restic.DataBlob, b, restic.ID{}, false)
			if err != nil {
				return err
			}
			if _, ok := blobs[id]; !ok {
				blobs[id] = b
				pool = append(pool, id)
			}
		}
		save := func(nodes []*data.Node) (restic.ID, error) {
			sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
			return data.SaveTree(ctx, up, func(yield func(data.NodeOrError) bool) {
				for _, n := range nodes {
					if !yield(data.NodeOrError{Node: n}) {
						return
					}
				}
			})
		}
		stamp := func() time.Time {
			switch rnd.Intn(20) {
			case 0:
				return time.Time{}
			case 1:
				return time.Unix(-rnd.Int63n(1<<31), rnd.Int63n(1e9))
			default:
				return time.Unix(1_600_000_000+rnd.Int63n(1<<27), rnd.Int63n(1e9))
			}
		}
		inode := uint64(1000)
		dir := func(name string, sub restic.ID) *data.Node {
			inode++
			return &data.Node{Name: name, Type: data.NodeTypeDir, Mode: os.ModeDir | 0o755, Subtree: &sub, ModTime: stamp(), AccessTime: stamp(),
				ChangeTime: stamp(), UID: uint32(rnd.Intn(3)) * 1000, GID: 65532, Inode: inode, Links: 2}
		}
		leaf := func(ti, mi int) []*data.Node {
			var nodes []*data.Node
			for j := 0; len(nodes) < spec.leaf; j++ {
				inode++
				name := fmt.Sprintf("e%05d", j)
				if j%31 == 7 {
					name = fmt.Sprintf("raw-\xff\xfe\x80-%d", j) // not UTF-8
				}
				n := &data.Node{Name: name, Mode: 0o644, ModTime: stamp(), AccessTime: stamp(), ChangeTime: stamp(),
					UID: uint32(rnd.Intn(70000)), GID: uint32(rnd.Intn(70000)), Inode: inode, DeviceID: 2049, Links: 1}
				switch k := j % 101; {
				case k == 3:
					n.Type, n.Mode, n.LinkTarget = data.NodeTypeSymlink, os.ModeSymlink|0o777, "../target/"+name
				case k == 5:
					n.Type, n.Mode, n.LinkTarget = data.NodeTypeSymlink, os.ModeSymlink|0o777, "raw\xc3\x28target"
				case k == 11 && mi%7 == 0:
					n.Type, n.Mode = data.NodeTypeFifo, os.ModeNamedPipe|0o600
				case k == 13 && mi%7 == 1:
					n.Type, n.Mode = data.NodeTypeSocket, os.ModeSocket|0o755
				case k == 17 && mi%7 == 2:
					// Linux makedev(0x1234, 0x56789).
					n.Type, n.Mode, n.Device = data.NodeTypeCharDev, os.ModeDevice|os.ModeCharDevice|0o666, 0x0000_1000_5672_3489
				case k == 19 && mi%7 == 3:
					// Linux makedev(8, 1).
					n.Type, n.Mode, n.Device = data.NodeTypeDev, os.ModeDevice|0o660, 8<<8|1
				default:
					n.Type = data.NodeTypeFile
					switch j % 9 {
					case 0:
						n.Mode = 0o4755 | os.ModeSetuid
					case 1:
						n.Mode = 0o2775 | os.ModeSetgid | os.ModeSticky
					}
					n.Mode = n.Mode.Perm() | n.Mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky)
					for c := rnd.Intn(4); c > 0; c-- {
						id := pool[rnd.Intn(len(pool))]
						n.Content = append(n.Content, id)
						n.Size += uint64(len(blobs[id]))
					}
				}
				if j%13 == 0 {
					n.ExtendedAttributes = []data.ExtendedAttribute{{Name: "user.b", Value: []byte{0, 1, 2, 0xff}}, {Name: "user.a", Value: []byte(name)}}
					if j%26 == 0 {
						n.ExtendedAttributes = append(n.ExtendedAttributes, data.ExtendedAttribute{Name: "trusted.lazyfill", Value: []byte("stale")})
					}
				}
				nodes = append(nodes, n)
				if j%97 == 41 && n.Type == data.NodeTypeFile && len(nodes)+2 <= spec.leaf {
					// A hard-link group of three names.
					n.Links = 3
					for _, suffix := range []string{"-hl1", "-hl2"} {
						alias := *n
						alias.Name = name + suffix
						alias.AccessTime = stamp() // atime may differ within a group
						nodes = append(nodes, &alias)
					}
				}
			}
			return nodes
		}
		var tops []*data.Node
		for ti := 0; ti < spec.top; ti++ {
			var mids []*data.Node
			for mi := 0; mi < spec.mid; mi++ {
				sub, err := save(leaf(ti, mi))
				if err != nil {
					return err
				}
				mids = append(mids, dir(fmt.Sprintf("m%04d", mi), sub))
			}
			sub, err := save(mids)
			if err != nil {
				return err
			}
			tops = append(tops, dir(fmt.Sprintf("t%04d", ti), sub))
		}
		empty, err := save(nil)
		if err != nil {
			return err
		}
		tops = append(tops, dir("empty-dir", empty))
		root, err = save(tops)
		return err
	}))
	id, err := data.SaveSnapshot(ctx, repo, &data.Snapshot{Tree: &root, Time: time.Unix(1_700_000_000, 0), Paths: []string{"/scan"}})
	rtest.OK(t, err)
	return id, blobs
}

// latencyBackend delays and counts pack reads, to model object storage.
type latencyBackend struct {
	backend.Backend
	delay time.Duration
	count *int64
	block chan struct{} // when not nil, pack loads wait for it to close
}

func (b *latencyBackend) Load(ctx context.Context, h backend.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	if h.Type == backend.PackFile && ctx.Err() == nil {
		if b.count != nil {
			atomic.AddInt64(b.count, 1)
		}
		if b.block != nil {
			select {
			case <-b.block:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if b.delay > 0 {
			select {
			case <-time.After(b.delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return b.Backend.Load(ctx, h, length, offset, fn)
}
