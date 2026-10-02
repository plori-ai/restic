package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
)

// Reuse map of the lazyfill protocol, version 1 (doc/plori-lazy-fill.md):
// a privileged classifier lists the regular files of a lazily filled working
// copy whose content is known without reading them. `backup
// --lazyfill-reuse-map-fd N --lazyfill-reuse-binding B` stores the listed
// blobs for those files and reads every other file.
const (
	lzfrMagic      = "LZFR"
	lzfrVersion    = 1
	lzfrMaxBinding = 1024
	lzfMaxBlobID   = 64
)

// lazyfillReuseEntry is one reuse record: the file at a path with this inode
// and size has exactly these blobs.
type lazyfillReuseEntry struct {
	ino, size uint64
	blobs     restic.IDs
	lengths   []uint64
}

// lazyfillReuse maps a path relative to the backup root (raw bytes) to its
// record.
type lazyfillReuse map[string]lazyfillReuseEntry

// errLazyfillMalformed is a reuse map that violates the format.
var errLazyfillMalformed = errors.New("malformed reuse map")

func lazyfillMalformed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errLazyfillMalformed, fmt.Sprintf(format, args...))
}

// lzfReader reads the framing shared by the lazyfill formats and hashes the
// header and record frames for the trailer digest.
type lzfReader struct {
	r   *bufio.Reader
	h   hash.Hash
	buf []byte
	n   uint64 // record frames read
}

func newLZFReader(r io.Reader, magic string, version uint64) (*lzfReader, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	got := make([]byte, len(magic))
	if _, err := io.ReadFull(br, got); err != nil || string(got) != magic {
		return nil, lazyfillMalformed("magic %q", got)
	}
	if v, err := binary.ReadUvarint(br); err != nil || v != version {
		return nil, lazyfillMalformed("%s version %d", magic, v)
	}
	return &lzfReader{r: br, h: sha256.New()}, nil
}

func (fr *lzfReader) next() (byte, []byte, error) {
	kind, err := fr.r.ReadByte()
	if err != nil {
		return 0, nil, lazyfillMalformed("no trailer: %v", err)
	}
	n, err := binary.ReadUvarint(fr.r)
	if err != nil || n > lzfmMaxFrame {
		return 0, nil, lazyfillMalformed("frame length %d", n)
	}
	if uint64(cap(fr.buf)) < n {
		fr.buf = make([]byte, n)
	}
	fr.buf = fr.buf[:n]
	if _, err := io.ReadFull(fr.r, fr.buf); err != nil {
		return 0, nil, lazyfillMalformed("frame body: %v", err)
	}
	switch kind {
	case lzfmFrameHeader, lzfmFrameRecord:
		fr.h.Write(binary.AppendUvarint([]byte{kind}, n))
		fr.h.Write(fr.buf)
		if kind == lzfmFrameRecord {
			fr.n++
		}
	case lzfmFrameTrailer:
	default:
		return 0, nil, lazyfillMalformed("frame kind %#x", kind)
	}
	return kind, fr.buf, nil
}

// lzfFields decodes a frame payload: tags in strictly increasing order, a
// repeated tag only where repeated allows it, each value of the type given
// by types[tag-1] ('u' uvarint, 'b' bytes).
func lzfFields(p []byte, types string, repeated uint64, fn func(tag, u uint64, b []byte) error) error {
	var last uint64
	for len(p) > 0 {
		t, n := binary.Uvarint(p)
		if n <= 0 || t == 0 || t > uint64(len(types)) {
			return lazyfillMalformed("field tag %d", t)
		}
		p = p[n:]
		if t < last || (t == last && t != repeated) {
			return lazyfillMalformed("field %d out of order", t)
		}
		last = t
		var u uint64
		var b []byte
		if types[t-1] == 'u' {
			u, n = binary.Uvarint(p)
		} else {
			var l uint64
			l, n = binary.Uvarint(p)
			if n > 0 && l <= uint64(len(p)-n) {
				b = p[n : n+int(l)]
				n += int(l)
			} else {
				n = 0
			}
		}
		if n <= 0 {
			return lazyfillMalformed("value of field %d", t)
		}
		p = p[n:]
		if err := fn(t, u, b); err != nil {
			return err
		}
	}
	return nil
}

// readLazyfillReuse decodes a complete reuse map and checks its binding. Any
// error means that no file may be reused. Records whose blob IDs are not
// restic blob IDs are left out: their files are read.
func readLazyfillReuse(r io.Reader, binding []byte) (lazyfillReuse, error) {
	fr, err := newLZFReader(r, lzfrMagic, lzfrVersion)
	if err != nil {
		return nil, err
	}
	kind, p, err := fr.next()
	if err != nil {
		return nil, err
	}
	if kind != lzfmFrameHeader {
		return nil, lazyfillMalformed("first frame %q", kind)
	}
	var got []byte
	if err := lzfFields(p, "b", 0, func(_, _ uint64, b []byte) error {
		if len(b) > lzfrMaxBinding {
			return lazyfillMalformed("binding of %d bytes", len(b))
		}
		got = bytes.Clone(b)
		return nil
	}); err != nil {
		return nil, err
	}
	m := lazyfillReuse{}
	for {
		kind, p, err := fr.next()
		if err != nil {
			return nil, err
		}
		if kind == lzfmFrameTrailer {
			if err := checkLZFCountTrailer(fr, p); err != nil {
				return nil, err
			}
			break
		}
		if kind != lzfmFrameRecord {
			return nil, lazyfillMalformed("frame %q", kind)
		}
		var path []byte
		var e lazyfillReuseEntry
		var seen uint64
		restic32 := true
		var sum uint64
		err = lzfFields(p, "buub", 4, func(t, u uint64, b []byte) error {
			seen |= 1 << t
			switch t {
			case 1:
				path = b
			case 2:
				e.ino = u
			case 3:
				e.size = u
			case 4:
				id, length, err := lzfBlob(b)
				if err != nil {
					return err
				}
				sum += length
				if len(id) != len(restic.ID{}) {
					restic32 = false
					return nil
				}
				e.blobs = append(e.blobs, restic.ID(id))
				e.lengths = append(e.lengths, length)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if seen&0b11110 != 0b11110 || e.size == 0 || sum != e.size {
			return nil, lazyfillMalformed("record %q needs a path, an inode, a size and blobs summing to it", path)
		}
		if err := checkRawPath(path); err != nil || len(path) == 0 {
			return nil, lazyfillMalformed("record path %q", path)
		}
		if _, ok := m[string(path)]; ok {
			return nil, lazyfillMalformed("duplicate path %q", path)
		}
		if !restic32 {
			e = lazyfillReuseEntry{} // listed, never reused
		}
		m[string(path)] = e
	}
	if _, err := fr.r.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, lazyfillMalformed("data after trailer")
	}
	if !bytes.Equal(got, binding) {
		return nil, errors.New("reuse map binding does not match --lazyfill-reuse-binding")
	}
	return m, nil
}

// lzfBlob decodes a blob value: uvarint ID length, ID, uvarint plaintext
// length.
func lzfBlob(b []byte) ([]byte, uint64, error) {
	l, n := binary.Uvarint(b)
	if n <= 0 || l == 0 || l > lzfMaxBlobID || l > uint64(len(b)-n) {
		return nil, 0, lazyfillMalformed("blob ID")
	}
	id, rest := b[n:n+int(l)], b[n+int(l):]
	length, m := binary.Uvarint(rest)
	if m <= 0 || m != len(rest) || length == 0 {
		return nil, 0, lazyfillMalformed("blob length")
	}
	return id, length, nil
}

// checkLZFCountTrailer checks the count-and-digest trailer.
func checkLZFCountTrailer(fr *lzfReader, p []byte) error {
	var count uint64
	var digest []byte
	var seen int
	if err := lzfFields(p, "ub", 0, func(t, u uint64, b []byte) error {
		seen++
		if t == 1 {
			count = u
		} else {
			digest = b
		}
		return nil
	}); err != nil {
		return err
	}
	if seen != 2 || count != fr.n || !bytes.Equal(digest, fr.h.Sum(nil)) {
		return lazyfillMalformed("trailer does not match the records read")
	}
	return nil
}

// reuser returns the archiver's ReuseContent function: a file's listed blobs
// are used only when its inode and size match the record and every blob is
// in the repository index with the listed length. An empty map reuses
// nothing.
func (m lazyfillReuse) reuser(repo interface {
	LookupBlobSize(restic.BlobType, restic.ID) (uint, bool)
}) func(snPath string, fi *fs.ExtendedFileInfo) (restic.IDs, bool) {
	return func(snPath string, fi *fs.ExtendedFileInfo) (restic.IDs, bool) {
		e, ok := m[strings.TrimPrefix(snPath, "/")]
		if !ok || len(e.blobs) == 0 || e.ino != fi.Inode || fi.Size < 0 || e.size != uint64(fi.Size) {
			return nil, false
		}
		for i, id := range e.blobs {
			if n, ok := repo.LookupBlobSize(restic.DataBlob, id); !ok || uint64(n) != e.lengths[i] {
				return nil, false
			}
		}
		return append(restic.IDs(nil), e.blobs...), true
	}
}
