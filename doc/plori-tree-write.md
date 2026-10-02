# Plori tree writer (`serve-write`), protocol version 1

`restic serve-write` writes standard restic snapshots from an existing tree plus
edits, or from a merge plan, without a filesystem. It serves the read endpoints
of `serve-read` ([plori-serve-read.md](plori-serve-read.md)) and the lazy-fill
endpoints `/skeleton` and `/v1/read` ([plori-lazy-fill.md](plori-lazy-fill.md))
on the same socket.
The repository format does not change: stock restic reads, checks, restores and
prunes what it writes.

```
restic -r REPOSITORY serve-write --socket /private/dir/write.sock \
    --trash-dir .plori-trash \
    --public-exclude lost+found --public-exclude '.nfs*' --public-exclude .forge \
    --public-exclude .trash --public-exclude .plori-trash --public-exclude .plori-workspace \
    --public-exclude .control --public-exclude .config --public-exclude .jfs \
    --public-exclude .stats --public-exclude .accesslog
```

The names above are the Plori platform's values and serve as an example. The
command has no built-in excludes, trash directory, owner, host name or tags;
the caller supplies all of them.

| Flag | Meaning |
|---|---|
| `--socket PATH` | Unix socket, published with mode 0600. The parent directory must belong to the caller. An existing path is refused. |
| `--public-exclude PATTERN` | Name pattern (`path.Match`, compared in lower case) that the public twin leaves out at every depth. Repeatable. No default. |
| `--trash-dir NAME` | Root directory name that the `trash`, `restore` and `empty-trash` edits use. Without it, those edits are refused. |
| `--max-request-bytes N` | Body limit of `/tree-write` and `/verify-write` (default 96 MiB, minimum 1 MiB). |
| `--read-workers`, `--read-memory-bytes`, `--read-cache-bytes` | Bounds of `/v1/read`, as for `serve-read` ([plori-lazy-fill.md](plori-lazy-fill.md)). |

Repository, password, backend credentials and cache flags are the ordinary
restic options and environment. No request selects a repository, a password, a
credential or a host path: the process configuration does.

## Process, locks and index

- The process holds **no repository lock while idle**. Reads (`/tree`, `/walk`,
  `/file`, `/snapshots`, `/prepare`, `/prepare-write`) take no lock, as with
  `serve-read --no-lock`. `--no-lock` itself is refused.
- Each `/tree-write` takes upstream's shared (append) lock, as `restic backup`
  does, and releases it before the answer. Upstream waits 200 ms between its two
  lock checks, so every write costs at least 200 ms. An exclusive lock (prune,
  `forget --prune`) refuses the write with 503 `repository_locked`; the caller
  retries. A lost lock cancels the write.
- Under the lock, the writer lists the index files. If the listing differs from
  the files it knows, it loads the index (upstream's incremental load). If an
  index file disappeared, a prune ran: the writer also drops its projection
  cache, its content-token index and its loaded snapshots. After its own upload
  it records the listing again.
- Requests are serialized, reads included, as in `serve-read`, except
  `/skeleton` and `/v1/read`: they run concurrently with each other and with
  writes, and take the request gate only to reload the index after a lookup
  miss ([plori-lazy-fill.md](plori-lazy-fill.md)).
- SIGINT and SIGTERM cancel running requests, stop the listener, wait until
  every running write returned and removed its lock file, and then exit.
- A failed upload leaves upstream's uploader state unusable
  (`Repository.WithBlobUploader` does not reset it). The next write opens the
  repository again in the same process. Packs that the failed request stored
  without an index entry are orphans that prune removes.

- Start-up opens the repository twice at the same time (writer and verifier
  handles, each with its own key derivation) and loads the writer's index. On
  the 290k-entry local test repository the socket was ready after 0.66–0.68 s,
  as with `serve-read`.

## Versioning

Every request body carries `"version": 1`. Another version is refused with 400
`unsupported_version`. JSON decoding refuses unknown fields and trailing data,
so a newer client cannot have a field silently ignored. `GET /version` answers:

```json
{"protocol":"tree-write","version":1,"restic":"0.19.1-dev","endpoints":["/version","/prepare-write","/tree-write","/verify-write","/prepare","/tree","/walk","/file","/snapshots","/skeleton","/v1/read"],"trash_dir":".plori-trash","public_excludes":["lost+found", "..."]}
```

A caller probes `/version` and refuses to admit writes when the protocol or
version is not the one it implements.

## `POST /prepare-write`

`{"version":1,"base":{"snapshot":ID}}` computes the statistics of the
snapshot's tree (entry counts, inode maximum, hard-link names), so the first
write on it does not walk the whole tree. It writes nothing and takes no lock.
The answer has the shape of a `/tree-write` answer with `head` set to the
snapshot's tree and counts.

## `POST /tree-write`

```json
{
  "version": 1,
  "base": {"snapshot": "64-hex"} | {"empty": true},
  "time": "2026-10-02T10:00:00.123456789Z",
  "owner": [65532, 65532],
  "hostname": "plori-workspace",
  "tags": ["plori-op:<uuid>", "plori-attempt:3"],
  "public_tags": ["plori-public"],
  "paths": ["/scan"],
  "edits": [ ... ] | "merge": {"sources": [...], "entries": [...]},
  "contents": [{"length": 5, "sha256": "64-hex", "data": "aGVsbG8="}]
}
```

| Field | Rule |
|---|---|
| `base` | Exactly one of a full lower-case snapshot ID and `"empty": true`. |
| `time` | RFC 3339 with nanoseconds. It is the mtime and ctime of created and changed nodes. The same request on the same base writes the same trees. |
| `owner` | UID and GID of created nodes. Required. |
| `hostname`, `tags`, `public_tags` | Snapshot metadata. The head has `tags`; the twin has `tags` followed by `public_tags`. 1 to 64 tags in total, without `,`, NUL or newline. Tags support discovery only. |
| `paths` | Snapshot paths. Required with an empty base; refused with a base snapshot, whose paths, user name, UID and GID the result copies. The head's parent is the base snapshot. |
| `edits` or `merge` | Exactly one. |
| `contents` | Up to 4096 items. `data` is base64 inside the JSON body. The server refuses an item whose bytes do not match `length` and `sha256`. |

### Edits

At most 1024 edits, applied in order; each edit sees the result of the earlier
ones. Paths are relative, slash-separated, canonical, at most 128 components,
without `..` or a `lost+found` component. An existing symbolic link as a path
component refuses the edit; no edit follows a link. `expected_etag` is optional:
absent skips the check, `0` requires that the path does not exist (write only),
any other value must equal the ETag of the stored node. Each op takes only its
own fields; any other field is 400.

| Op | Fields | Effect |
|---|---|---|
| `write` | `path`, `content` (index), `mode`?, `expected_etag`? | A file with several names is rewritten in place: content, size, mtime and ctime (and mode bits 0777 when given) change on every name of the inode, private and trash names included; the inode stays. Otherwise a new node replaces the path: new inode, one link, `owner`, mode = `mode`, else the replaced file's permission bits, else 0644. Missing parents are created with mode 0755 and `owner`. Writing over a directory or symlink is `content_refused`. |
| `mkdir` | `path`?, `mode`? | Creates the directory and its parents (mode, default 0755). An existing directory is success without a change; an empty path is a no-op. |
| `rename` | `path`, `to`, `expected_etag`? | Moves a file or directory. The inode stays; its ctime changes on every name. An occupied destination is `file_exists`. |
| `chmod` | `path`, `mode` (07777)?, `expected_etag`? | Sets permission, setuid, setgid and sticky bits and the ctime of the inode, on all names. |
| `trash` | `path`, `handle`, `expected_etag`? | Moves a file or directory to `<trash-dir>/<handle>`. A missing trash directory is created with mode 0700, owner 0:0. Receipt `deleted` is 1. |
| `restore` | `path`, `handle` | Moves `<trash-dir>/<handle>` back to `path` (no ETag check). An occupied path is `file_exists`. |
| `empty-trash` | none | Removes the trash directory. Receipt `deleted` is the number of entries directly in it. A hard-linked file with names outside the trash keeps them with the new link count and a new ctime; a file left with one name has device ID 0, as `backup` stores single-link files. |

Missing paths give `file_not_found`, or `file_stale` when `expected_etag` was
set. A non-directory parent gives `file_not_dir`.

### Merge plan

```json
"merge": {"sources": [{"snapshot": "64-hex"}], "entries": [
  {"path": "src/a.go", "kind": "file", "mode": 420, "size": 120, "digest": "restic:<64-hex>"},
  {"path": "src", "kind": "dir", "mode": 493},
  {"path": "link", "kind": "symlink", "mode": 511, "size": 8, "target": "src/a.go"},
  {"path": "x", "kind": "file", "mode": 420, "size": 9, "digest": "restic:...", "link_group": "<label>"}
]}
```

The result tree is exactly the entry list (at most 1,000,000 entries; an empty
list writes the empty tree). Every entry needs a directory entry for its parent.
Entries with a name that matches `--public-exclude` are refused. A link group
has at least two names with equal size, mode and digest. `mode` holds bits 07777.

- A name whose base node has the same kind, content, size, link group (label
  computed from the base's names) and symlink target keeps that native node with
  every field. The entry's mode and the request owner are applied; a change
  sets the ctime. A directory whose nodes are all kept keeps its subtree ID.
- Any other node is new: a new inode (one per new link group, `links` = group
  size), mtime and ctime `time`, the entry's mode and the request owner.
- A file digest `restic:<hex>` is the SHA-256 of the ordered blob IDs. The
  content comes from the base or a source node at the same path with that token,
  else from any file of the sources with that token. A plain SHA-256 digest names
  a request content item. Every blob must be in the index with sizes that sum to
  the entry size.
- A directory whose names changed gets mtime and ctime `time`.

A conflicted comparison writes the same way: the plan lists current's node for a
conflicted path, and the pair is written.

### Answer

```json
{
  "version": 1,
  "head": {"snapshot": "64-hex", "tree": "64-hex", "entries": 289950, "logical_bytes": 72249200},
  "public": {"snapshot": "...", "tree": "...", "entries": 289626, "logical_bytes": 72164246},
  "incomplete_link_groups": ["/a/x"],
  "edits": [{"path": "a/b", "etag": 123, "before_etag": 456, "deleted": 0}],
  "contents": [{"ids": ["64-hex", "..."]}],
  "data_added": 2311, "data_added_packed": 1418,
  "timings_ms": {"lock": 207.1, "index_list": 0.5, "build": 2.1, "upload": 17.0, "total": 27.0, "unlock": 0.1}
}
```

- `head` is the lossless result. `{"empty": true}` means the empty tree: no
  snapshot is written and no ID is given.
- `public` is the twin, the head without names matching `--public-exclude` at
  any depth. `{"empty": true}` when only excluded names remain. `public` is
  absent when the twin is refused: a hard-link group has a public name and an
  excluded name. `incomplete_link_groups` then lists the first public name of
  each such group. A head whose own groups are incomplete is an error.
- `entries` counts every node except the root; `logical_bytes` sums file sizes
  over all names, as `restic ls --json` reports them.
- `edits[i].path` names the node whose `etag` is reported (empty for `trash`
  and `empty-trash`). `before_etag` is the ETag of the node at the edit's `path`
  before the edit (absent for `restore`, `empty-trash` and a new path). The ETag is FNV-1a 64 of the decimal string
  `inode-size-mtimeSec.mtimeNsec-ctimeSec.ctimeNsec`, with 0 mapped to 1.
- `contents[i].ids` are the blob IDs of request content `i`; `/verify-write`
  needs them.
- `data_added` and `data_added_packed` count new blobs of this request
  (plaintext and stored bytes), as `backup` does. The head snapshot's summary
  carries them; the twin's summary carries none, so they are counted once.

Order of work: all checks and the complete result are computed before anything
is stored. Then one upload stores the new data and tree blobs of head and twin
and writes the index; then the head snapshot, then the twin snapshot. A failure
after the upload can leave packs, an index or a head snapshot without its twin;
these are not a success and are tagged for discovery.

### Errors

| Status | Body | Meaning |
|---|---|---|
| 409 | `{"code","edit","current_etag"?}` | A precondition did not hold; nothing was written. Codes: `file_stale` (with `current_etag` when the node exists), `file_not_found`, `file_exists`, `file_not_dir`, `content_refused`. `edit` is the index of the refused edit. |
| 400 | `{"code":"invalid_request","detail"}` or `unsupported_version` | Malformed request; nothing was written. Includes merge entries whose content no source holds. |
| 404 | `{"code":"snapshot_not_found"}` | A base or source snapshot does not exist. |
| 503 | `repository_locked`, `repository_unavailable`, `draining` | Retry later. |
| 500 | `{"code":"write_failed"}` | The write failed, for example a tree that cannot be re-encoded without loss (a node field unknown to this restic version). The process stays usable. |

## `POST /verify-write`

```json
{"version": 1, "request": { tree-write request without content data }, "result": { tree-write answer }}
```

The verifier uses a second repository handle, opened without the local cache,
with its own index, blob cache and statistics. It does not use anything the
writer holds. It answers `{"version":1,"ok":true,"timings_ms":{...}}` or
`{"version":1,"ok":false,"code","detail"}`. Checks, in order:

1. The handle's index matches the index files the backend lists now (same rule
   as the writer; a removed index file reloads it).
2. Each content item's blob IDs are in the index, their bytes are read from the
   backend, and length and SHA-256 equal the request's.
3. The request is replayed on its base with those blob IDs. The replay's head
   tree, twin tree, empty markers, refused groups and edit receipts must equal
   the result (`plan_mismatch`, `source_missing`).
4. The trees the replay produced are dropped from memory and read again from the
   backend, without the blob cache. Both trees are walked; every subtree and
   content blob below them must be in the index (`blob_missing`,
   `tree_unreadable`), the counts must equal the result (`count_mismatch`), and
   the twin may not contain an excluded name (`projection_mismatch`).
5. Both snapshots are loaded by ID from the backend; when a load fails, a
   backend listing tells a missing snapshot (`snapshot_missing`) from a backend
   failure. Tree, parent (base for the head, head for the twin),
   host name, paths and tags must equal what the request writes, and the head
   summary must record `data_added` and `data_added_packed`
   (`snapshot_mismatch`).

The walk is incremental. The verifier keeps the statistics of a tree only when
it loaded that tree itself and found every blob below it in its own index.
Trees are immutable and index files are only added between prunes, so such a
tree needs no new walk until an index file disappears, which drops all
statistics. Trees that a replay produced never outlive the call; the
candidate's new trees are always read from the backend.

A backend failure during verification answers 503 `verify_unavailable`; a
definite mismatch answers 200 with `ok: false`. A read failure of a candidate
tree is reported as `tree_unreadable`, which a backend outage can also cause;
the caller may run the verification again.

## Limits

96 MiB request body (flag), 1024 edits, 4096 contents, 64 tags, 1,000,000
merge entries, 128 path components. A shared 64 MiB blob cache per handle, tree
statistics for up to 2^21 trees, 1024 loaded snapshots and 8 content-token
indexes; each memo is cleared when full. These do not bound total process
memory: inline content and a merge plan are held in memory during a request.

## Measurements

Local filesystem repository on a shared 16-core host, one persistent
connection, warm process (`run.sh` of the B-PROTO harness, adapted to this
protocol). Not an SLO; object storage adds a request round trip per upload
stage (data and tree packs in parallel, then the index, the head snapshot and
the twin snapshot).

| Tree | Series | Write p50 / p95 ms | Verify p50 / p95 ms | Write phases (median ms) |
|---|---|---|---|---|
| 50k entries | 30 overwrites | 236 / 258 | 92 / 96 | lock 207, build 2, upload 17 |
| 290k entries | 30 overwrites | 237 / 392 | 139 / 172 | lock 208, build 2, upload 18 |
| 290k entries | 20 renames | 242 / 747 | 143 / 153 | lock 207, build 4, upload 20 |
| 290k-entry merge plan | 10 writes | 2888 (p50) | 2995 (p50) | build 1885 |

The first verification of a 290k-entry tree in a process walks it once
(1.1 s). Later verifications load the new index file (about 115 ms, mostly
upstream's `runtime.GC` in `LoadIndex`) and walk the changed paths. Write
outliers of up to 4.8 s were in the local backend's lock-file, pack and
snapshot writes while other jobs loaded the host (load average about 11).
