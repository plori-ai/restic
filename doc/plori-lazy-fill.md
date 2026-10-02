# Plori lazy-fill endpoints (`/skeleton`, `/v1/read`), protocol version 1

A lazy working copy is a native local tree that has every name and all
metadata of a snapshot, while the bytes of regular files are filled from the
repository on first access. `restic serve-read` and `restic serve-write` serve
the two inputs of such a copy on their Unix socket:

- `GET /skeleton?snapshot=ID` streams the snapshot's metadata in the lazyfill
  metadata-stream format (`LZFM`, version 1).
- `POST /v1/read` serves byte ranges of content blobs in the lazyfill
  content-source HTTP binding (version 1).

Both formats are defined by the lazyfill protocol (Plori `services/lazyfill`,
`PROTOCOL.md` version 1, sections 2, 3 and 5). This document repeats what a
client of this socket needs. The repository format does not change.

## `GET /skeleton?snapshot=ID`

`ID` is the full lower-case snapshot ID. Errors before the stream starts are
plain HTTP errors, as for the other read endpoints: 400 for a malformed ID or
a request body, 404 when the snapshot does not exist, 405 for another method,
500 otherwise. A success is 200 with `Content-Type: application/x-lazyfill-meta`
and the stream as the body. A failure after the stream started ends it with an
error trailer frame; the HTTP status stays 200. A consumer accepts the stream
only when it reaches a trailer whose counts and digest match.

### Framing

```
stream  = "LZFM" version(uvarint 1) header-frame record-frame* trailer-frame EOF
frame   = kind(1 byte: 'H', 'R' or 'T') length(uvarint) payload(length bytes)
payload = field*            field = tag(uvarint) value
```

Values are `uint` (uvarint), `sint` (zigzag varint) or `bytes` (uvarint length
and bytes). Tags are in strictly increasing order; tags 14 and 15 of a record
repeat. A payload is at most 16 MiB. The trailer digest is SHA-256 over every
header and record frame as sent (kind byte, length uvarint, payload).

Header: tag 1 `source` (bytes: the 32-byte snapshot ID), tag 2 `root` (bytes:
the 32-byte root tree ID), tag 3 `producer` (bytes: `restic <version>
serve-read /skeleton`).

Record, one per tree node, in walk order: a directory before its entries,
entries in the tree's name order. There is no record for the root directory.

| Tag | Name | Type | Value |
|---|---|---|---|
| 1 | path | bytes | Node names from the root joined by `/`, raw bytes as `data.Node.Name` holds them (no UTF-8 replacement, no normalization). |
| 2 | type | uint | 1 dir, 2 file, 3 symlink, 4 fifo, 5 chardev, 6 blockdev (`dev`), 7 socket. |
| 3 | mode | uint | `Mode.Perm()` plus setuid 04000, setgid 02000, sticky 01000. |
| 4, 5 | uid, gid | uint | `UID`, `GID`. |
| 6, 7 | mtime, atime | sint | `ModTime.UnixNano()`, `AccessTime.UnixNano()`: the values `restic restore` passes to `utimensat`. |
| 8 | size | uint | Files only: `Size`. |
| 9, 10 | dev_major, dev_minor | uint | Character and block devices only: `Device` split with the Linux `dev_t` encoding (`unix.Major`, `unix.Minor` on Linux), on every platform. |
| 11 | link_target | bytes | Symlinks only: `LinkTarget` as raw bytes (`linktarget_raw` already decoded). |
| 12, 13 | link_group, link_count | uint | Non-directories with `Links > 1`: a number per (`DeviceID`, `Inode`) pair, assigned 1, 2, ... in walk order, and `Links`. |
| 14 | xattr | bytes, repeated | `ExtendedAttributes` in stored order; value: uvarint name length, name, uvarint value length, value. |
| 15 | blob | bytes, repeated | Files: `Content` in order; value: uvarint 32, the blob ID, uvarint plaintext length from the repository index. |

Trailer: tags 1 to 6 count records, directories, files (hard-link aliases
included), symlinks, other nodes, and the sum of file sizes; tag 7 is the
digest. An error trailer has only tag 8, the failure text.

The producer does not filter extended attributes (the skeleton builder drops
its own reserved names). It ends the stream with an error trailer, rather than
emit a record that a builder could not create exactly, when a node:

- has a type outside the table (`irregular`, an empty type);
- has a name that is empty, `.`, `..`, longer than 255 bytes, or contains `/`
  or NUL, or a path longer than 4095 bytes;
- is a symlink whose target is empty, longer than 4095 bytes or contains NUL;
- is a directory without a subtree, or a non-file with content;
- is a file whose content blobs are not in the index, or whose blob lengths do
  not sum to its size;
- has an extended attribute whose name is empty, longer than 255 bytes or
  contains NUL, or whose value is over 64 KiB.

Trees are read from the repository (its local cache when configured), not
through the tree cache of the other read endpoints, so a skeleton does not
evict the trees that Files reads and writes use. While the walk emits a
directory, the trees of its next 8 subdirectories load in the background, at
most 8 at once per request. Memory is bounded by the depth of the tree times
9 directories, plus one number per hard-linked inode.

Without a repository cache (`--no-cache`) every directory is one ranged
backend read: a snapshot with 30,000 directories needs 30,000 reads. With the
cache, the first read of a tree pack downloads the pack once and later trees
of that pack are local.

## `POST /v1/read`

Headers: `Lazyfill-Version: 1` (required), `Content-Type: application/json`,
and optionally `Lazyfill-Timeout-Ms: N` (a positive integer: the milliseconds
left before the caller's deadline; the server applies it as its own deadline).
Body, at most 16 MiB, unknown fields refused:

```json
{"source":"<64 hex>","path":"<base64 std, padded, raw path>","file_size":N,
 "offset":N,"length":N,
 "spans":[{"blob":"<64 hex>","offset":N,"length":N}]}
```

- `source` is a full lower-case snapshot ID. `offset + length` must not exceed
  `file_size`.
- With `spans`, the server sends exactly those blob ranges in order; their
  lengths must sum to `length`, and each must lie inside its blob's plaintext
  length from the index. `path` is then informational.
- Without `spans`, `path` names a regular file of `source` (relative, raw
  bytes, no symlink followed), whose size must equal `file_size`; the server
  maps `[offset, offset+length)` onto the file's content blobs.

Success: 200, `Content-Type: application/octet-stream`, a chunked body of
exactly `length` bytes, and the trailer `Lazyfill-Status: ok`. The header is
sent with the first byte. A failure after it ends the body and sets the
trailer to `error <code> <message>`; a client treats a missing or non-`ok`
trailer, or a byte count other than `length`, as a failed read. A failure
before the first byte is a JSON body `{"code":"...","message":"..."}`:

| Code | Status | Here |
|---|---|---|
| `bad_request` | 400 | malformed body, IDs that are not full lower-case hex, ranges outside the file or blob, bad timeout header |
| `unsupported_version` | 400 | `Lazyfill-Version` missing or not `1` |
| `not_found` | 404 | a span's blob is not in the index after one index reload; path mode: snapshot or file missing, or not a regular file |
| `corrupt` | 502 | a blob failed authentication, or its plaintext length differs from the index |
| `unavailable` | 503 | any other blob load failure (the repository's backend and hash-mismatch errors are not typed) |
| `deadline` | 504 | the request's deadline passed |
| `internal` | 500 | not used |

`denied` (403) is not produced. The socket is platform-only and serves any
blob of the repository by ID: whether a caller may read a source, and whether
a blob belongs to that source, is decided by the node content source that
forwards guest requests to this socket.

### Concurrency and bounds

`/v1/read` and `/skeleton` do not take the request gate that serializes the
other endpoints and every `serve-write` request. Content reads:

- share one load per blob: concurrent requests for a blob wait on the same
  load (a flight), each within its own deadline. A load runs while at least
  one request waits for it; when the last one leaves, it is cancelled, and a
  later request starts a new one. A failed load fails every waiting request.
- run at most `--read-workers` (default 16) backend blob loads at once. Each
  load is one ranged backend read; consecutive blobs of a pack are not merged
  into one read.
- hold at most `--read-memory-bytes` (default 256 MiB) of blobs fetched or
  being fetched and not yet sent. A request reads up to 8 blobs ahead of the
  one it sends, taking only memory that is free; it waits for memory only
  while it holds none, so requests never wait on each other's reservations.
- keep sent blobs in an LRU cache of `--read-cache-bytes` (default 32 MiB, 0
  disables it), separate from the 64 MiB tree cache.

Blob lookups and loads use the repository's thread-safe index. The index
loaded at start-up does not know packs written later, and after a prune it
names packs that are gone. When a lookup or load fails, the request takes the
request gate, reloads the index unless another request of these endpoints
reloaded it since this one started, releases the gate, and repeats the
operation once. Index loads are not safe concurrently with an upload or
another index load; the gate makes them exclusive. Requests that miss at the
same time share one reload. A request that misses while a write
or a gated stream (`/walk`) holds the gate waits for it within its deadline.
`serve-write` replaces its repository handle after a failed upload under a
lock that these endpoints read it through; a request that started on the old
handle finishes with it.

## Tests and measurements

`cmd/restic/serve_read_skeleton_test.go` and
`cmd/restic/serve_read_content_test.go` cover the endpoints. Generated
snapshots of 50,521 and 296,041 nodes (files of zero to three blobs, empty
files, symlinks with non-UTF-8 targets, three-name hard-link groups, extended
attributes including a stale `trusted.lazyfill`, non-UTF-8 names, FIFOs,
sockets, character and block devices, setuid/setgid/sticky, nanosecond,
pre-1970 and zero times) are streamed over a Unix socket, decoded, and
compared field by field with an independent `walker.Walk` and with
`restic ls --json`. Content tests read over a Unix socket and compare bytes.

The tests decode with a test decoder and read with a test client written
from this document, through two package variables (`lzfmDecode`,
`contentRead` in `serve_read_lazyfill_helpers_test.go`). A cross-check
replaces them with the lazyfill module's own `protocol.MetaDecoder` and
`source.Client`: a test file that sets the variables in `init` is added to
the package with a Go overlay, and the lazyfill module is resolved through a
Go workspace that uses both modules. Neither the file nor the module is part
of this repository, so `go mod tidy` and builds do not depend on them:

```
GOWORK=/path/go.work go test -overlay overlay.json -run 'TestServeRead(Skeleton|Content)' ./cmd/restic
```

Measurements on a 16-core host with `GOMAXPROCS=2`, a local repository and
its cache warm (single-host numbers, not an object-storage qualification):

| Case | Result |
|---|---|
| `/skeleton`, 50,521 nodes | 5.8 MB stream, 0.17–0.19 s |
| `/skeleton`, 296,041 nodes (in the test process) | 34.1 MB stream, 0.97–1.02 s |
| `/skeleton`, 296,041 nodes, `restic serve-read` binary and curl | 1.11–1.13 s; process peak RSS 47.3 MB (40.5 MB before the first request). With `GOMAXPROCS=16`: 0.39 s, 64.7 MB |
| `/skeleton` of 106 trees without a cache, 20 ms per backend read | 106 reads in 0.37 s (one tree at a time: at least 2.1 s) |
| 64 readers × 40 reads, 2 s deadline, 20 ms per backend pack read, 16 workers | 2,560 reads, 510 MiB in 5.1 s, 0 failures; latency p50 132–135 ms, p95 149–150 ms, p99 163–166 ms, max 176–187 ms; about 3,840 backend reads |
| 64 single-blob reads at once, 50 ms per backend read | 0.21 s (serialized reads would take 3.2 s) |
| 32 requests for one blob while the backend is blocked | one backend read; the request that left failed alone |
| 64 concurrent misses of blobs written after the index was loaded | one index reload |
