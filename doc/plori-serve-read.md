# Plori snapshot reader

Run `restic -r REPOSITORY serve-read --socket /private/directory/read.sock`.
Standard repository, password, cache and lock flags/environment apply. There is
no TCP listener. The platform must own the socket's parent directory and perform
workspace authorization before forwarding requests. The socket is published
with mode 0600; existing paths are refused. Unix socket support and filesystem
support for hard-linking a socket are required (tested on Linux).

The process opens one repository with `openWithReadLock`, loads its index, and
keeps the shared lock until shutdown. SIGTERM/SIGINT cancels requests, drains
HTTP for up to five seconds, removes the socket, and releases the lock. Stop
readers before exclusive maintenance. `--no-lock` has upstream semantics; the
platform must then exclude deletion/prune itself.

HTTP/1.1 API:

- `POST /prepare`, JSON `{"snapshot":"FULL_64_HEX_ID"}`: loads the snapshot and
  validates/warms its root tree; returns 204. Repeated preparation is idempotent.
- `GET /tree?snapshot=ID&path=/dir`: JSON array of original `data.Node` objects.
  Stored inode, device_id, links, mtime, ctime, linktarget and ordered content IDs
  retain upstream JSON encoding, including upstream omission of zero fields.
- `GET /walk?snapshot=ID`: NDJSON records `{"path":"/dir/file","node":{...}}`,
  using `walker.Walk`. Includes directory nodes, excludes the synthetic `/`
  node. Unlike `find --json`, content/subtree IDs and attributes are retained.
  A failed walk sets the HTTP trailer `X-Restic-Error: read failed`; consumers
  must read to EOF and check the trailer before accepting the traversal.
- `GET /file?snapshot=ID&path=/file&offset=N&length=M`: regular-file bytes.
  Omitted offset is zero; omitted length means through EOF. Length is clipped
  at EOF, zero length and offset at EOF return an empty body, offset beyond EOF
  returns 416. Query ranges return 200 with Content-Length; HTTP Range headers
  are not interpreted. Blobs before the offset are skipped using indexed sizes.
  Transfer failures abort the response; consumers must reject incomplete bodies.
- `GET /snapshots`: fresh backend listing on every request, as a JSON array with
  full `id` and upstream snapshot fields (plus upstream `short_id`).

Paths are absolute, canonical, slash-separated snapshot paths, not host paths;
URL-encode them. Symlinks are returned as nodes and never followed. Snapshot
selectors must be full IDs; `latest` and prefixes are rejected. An uncached ID
causes exactly one index refresh and direct snapshot load in that request,
without a discovery throttle. Failures return generic HTTP errors without
backend/path/credential details. Requests are serialized, including streams,
so index refresh cannot race repository reads. A long walk or slow reader can
delay other requests; the platform must account for this when scheduling reads.

The lazy-fill endpoints `GET /skeleton` (metadata stream of a snapshot) and
`POST /v1/read` (content blob ranges) are not serialized; they are described
in [plori-lazy-fill.md](plori-lazy-fill.md), with the flags `--read-workers`,
`--read-memory-bytes` and `--read-cache-bytes` that bound them.

Limits: 4 KiB prepare body, 8 KiB request URI, 16 KiB configured HTTP header
limit, 5 s header timeout, 10 s request read timeout, 5 min response deadline,
30 s idle timeout. Waiting requests honor cancellation. A shared 64 MiB blob
LRU warms trees and file data; up to 128 prepared root IDs are retained before
that map is cleared. This is not a total process memory bound. Preparation
warms the root, not every descendant or file. Cache eviction may require reads
again. No repository format or internal package is changed.

Local validation and measurement receipts are recorded by the implementation
report. Local filesystem latency does not qualify object-storage latency or
concurrent Files/merge workloads against the 300 ms target.

A Linux local-filesystem run with 50,000 files (254–257 bytes each), 100 leaf
directories and a single persistent HTTP connection measured prepare at
64.79 ms, directory-response p95 at 6.94 ms and file-first-byte p95 at 1.18 ms
(100 distinct leaf/file samples after prepare). Three complete walks emitted
50,100 nodes at 95,809–97,755 nodes/s. Process startup was 758.92 ms, excluded
from those request timings. These are single-host measurements, not an SLO.
