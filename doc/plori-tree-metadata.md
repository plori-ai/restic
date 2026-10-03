# Parent-relative stored tree metadata

Plori's restic 0.19.1 fork supports the opt-in backup option
`--tree-metadata-parent <full-snapshot-id>`. The argument must be an exact
64-character snapshot ID available in the repository. It is independent of
`--parent`, which remains the content-change-detection parent. Without the new
option the existing backup path is unchanged.

```sh
restic backup --parent "$image_stat_snapshot" \
  --tree-metadata-parent "$confirmed_head" --skip-if-unchanged .
```

The option inventories the metadata parent's nodes and stages completed current
nodes in memory. Content comes from the existing hashing or validated reuse
path. It never modifies live stat, `fileChanged`, or lazy reuse validation.
Trees are serialized only after the entire current hard-link partition is
known, then saved bottom-up. This uses O(nodes) memory and an additional parent
inventory; large-tree memory and latency have not been measured.

A file group reuses complete parent nodes only when both groups are complete,
contain exactly the same paths, and every peer has equal type, mode, numeric
owner, nanosecond mtime, size, links, xattrs, generic attributes, symlink target,
rdev and content references. Recorded atime is also compared. Native inode,
device ID, ctime, and owner names are excluded from this portable comparison.
Copying the whole parent node preserves its timestamp representation and owner
names. Metadata errors cannot establish equality. Independent equal-byte groups
remain separate, including across subtree boundaries.

Changed, split, joined, new, and incomplete file groups receive distinct nonzero
identities above the maximum inode and device IDs in both inventories. Exhaustion
refuses the backup. Inconsistent live hard-link peers and multiply linked files with an
unidentifiable `(0,0)` live identity also refuse the backup instead of guessing
topology or restoring one peer over another. Changed nodes with equal size/mtime
receive ctime at least one nanosecond above the parent's if live ctime did not
already advance; unrepresentable ctime refuses the backup. Directories are
compared after their child trees have been finalized. They reuse their complete
parent node only when the finalized subtree is equal too.

`--skip-if-unchanged` compares the finalized root with the metadata parent when
this option is supplied. An equal root emits the ordinary JSON summary with
zero added tree/data bytes and no `snapshot_id`. It does not create or retag a
snapshot. The snapshot's ordinary `parent` field, when a snapshot is created,
continues to describe the content parent. Progress file/directory classifications
still describe live nodes against that content parent; use tree equality and
blob/byte counts for the full-head no-op result.

Existing snapshot objects and repository JSON schemas are unchanged. The first
save without a confirmed head uses the ordinary native backup as its baseline.
This policy stabilizes later saves relative to that head, rather than assigning
a universal tree identity to independent imports.

## Required integration in the Plori repository (not implemented here)

The fork is insufficient to make ordinary run-end saves return `unchanged:true`.
The following work remains in paths owned by the runtime coordinator:

1. In `services/storage-worker/internal/workspacehelper/helper.go`, keep the
   selected content `--parent` as it is: lazy materialization S, eager raw image
   stat snapshot I, or confirmed head P. For a lossless head backup with a full
   `j.ParentSnapshot`, append `--tree-metadata-parent`, `j.ParentSnapshot`.
   Do not apply it to public backups or raw `plori-image` preparation snapshots.
   Change the skip guard from
   `j.SkipIfUnchanged && parent != "" && parent == j.ParentSnapshot` to a guard
   bound to the full confirmed P and lossless backup (independent of S/I).
   Admit a missing JSON snapshot ID only when that exact metadata comparison was
   requested; bind the resulting reuse receipt to P, never S/I. The parser must
   not treat a missing ID or a failed restic command as an unbound successful save.
2. In `services/control-plane/internal/workspacecontrol/restic_helper.go`, the
   current `job.Job.SkipIfUnchanged = l.Periodic && l.Parent != ""` gate must
   also request full-head comparisons for eligible run-end, stop, and recovery
   saves with an exact confirmed head. This is a verified receipt change, not
   merely enabling a command flag.
3. In `services/control-plane/internal/workspacecontrol/restic_save.go`, generalize
   the periodic-only missing-helper-snapshot-ID branch. Bind operation, attempt,
   exact confirmed head and expected sequence; verify that head and recheck it
   under normal concurrency/CAS rules. A concurrent advance must refuse/retry.
   Define explicit verified snapshot reuse so the old snapshot's lack of the
   current operation tag does not fail the ordinary new-snapshot tag check.
   Never retag the immutable old snapshot.
4. Preserve head/sequence and zero additional retained-byte charge on verified
   reuse, while preserving reservation release, attestation, owed-save
   settlement and required public publication/accept. An unchanged lossless head
   can still contain previously saved changes that require publication. Project
   public twins from the admitted immutable head, not from a re-statted copy.

Lazy-map live-inode/size/binding/blob-length checks remain necessary. Eager
`plori-image` snapshots must retain raw inode/ctime as stat hints. This option
eliminates unchanged lossless tree uploads; it does not eliminate raw image
snapshot storage or its retention cost. Stock compatibility, receipt concurrency,
lazy cold/fill/remount paths, staging and large-tree performance remain distinct
integration gates.

## Sources and alternatives

The base is Plori fork `42deff89455b583e71f348121493ab559160697f` (restic 0.19.1).
The binding implementation direction is RSCH-META section 5.A in runtime
`docs/design/workspaces/evidence/plo-1198/p4-stg11/tree-metadata.md`.

Maintainer/upstream evidence:

- [Restic #3004](https://github.com/restic/restic/issues/3004): ignore-inode
  changes content detection, not stored metadata (original report: 0.9.6).
- [Restic #4006](https://github.com/restic/restic/pull/4006): upstream's partial
  device-ID remedy, introduced in 0.17.0 and opt-in alpha in this 0.19.1 pin.
- [Borg 1.4.5 create](https://borgbackup.readthedocs.io/en/1.4.5/usage/create.html):
  archive ctime/atime policy is separate from its file cache's stat checks.

The implementation here is a parent-relative preservation policy, not a claim
that those upstream features solve complete hard-link or Workspace identity.
An additional cost-removal option from RSCH-META section 5.C is to replace raw
image-stat snapshots with a local attested, discardable stat/content descriptor
map. It could remove image tree uploads and their retention lifecycle entirely;
it requires separate binding, invalidation and performance evidence and is not
implemented or selected here.
