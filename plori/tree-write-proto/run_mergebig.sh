#!/usr/bin/env bash
# /merge-write with a full-size manifest: run_mergebig.sh ENTRIES (local repository)
# The public twin of a generated tree becomes the merge manifest with one file
# removed, one mode changed and one generated file added.
set -euo pipefail
S=$(cd "$(dirname "$0")" && pwd)
OUT=${WORK:-/tmp/tree-write-proto}
B=$S/bin
N=$1
W=$OUT/work-mergebig-$N
R=$OUT/results/mergebig-$N
rm -rf "$W" "$R"
mkdir -p "$W" "$R"
log() { echo "[$(date +%T)] $*" | tee -a "$R/run.log"; }
python3 "$S/gen_tree.py" "$W/tree" "$N" 43 | tee "$R/tree.json"
export RESTIC_PASSWORD
RESTIC_PASSWORD="$(head -c 24 /dev/urandom | base64)"
export RESTIC_CACHE_DIR=$W/cache RESTIC_REPOSITORY=$W/repo
STOCK=$B/restic-stock
$STOCK init -q
EXCL=(--iexclude lost+found --iexclude '.nfs*' --iexclude .forge --iexclude .trash --iexclude .plori-trash --iexclude .plori-workspace
	--iexclude .control --iexclude .config --iexclude .jfs --iexclude .stats --iexclude .accesslog)
H=$(cd "$W/tree" && $STOCK backup --json --host plori-workspace --tag plori-op:base . | jq -r 'select(.message_type=="summary") | .snapshot_id')
P=$(cd "$W/tree" && $STOCK backup --json --host plori-workspace --tag plori-op:base --tag plori-public --parent "$H" "${EXCL[@]}" . | jq -r 'select(.message_type=="summary") | .snapshot_id')
SOCK=/tmp/bproto-mergebig.sock
rm -f "$SOCK"
$B/restic-fork serve-write --socket "$SOCK" 2>"$R/serve-write.stderr" &
SWPID=$!
trap 'kill $SWPID 2>/dev/null || true' EXIT
for _ in $(seq 1 600); do [ -S "$SOCK" ] && break; sleep 0.1; done
log "prepare cold (twin): $($B/driver prepare -socket "$SOCK" -base "$P")"
log "merge-write: $($B/driver mergebig -socket "$SOCK" -base "$P" -n 10 -out "$R/mergebig.json")"
M=$(jq -r .last.head.snapshot "$R/mergebig.json")
jq -c '{entries, removed, chmodded, server: [.server_timings_ms[] | {merge_prefetch, merge_lookup, merge_nodes, merge_encode, build, flush, total}]}' "$R/mergebig.json" | tee -a "$R/run.log"
log "compare merge result vs twin (expect 4: removed, chmodded, merge-new, generated): $($B/driver compare -socket "$SOCK" -a "$M" -b "$P" | tee "$R/compare.txt" | tail -1)"
kill $SWPID; wait $SWPID || true
lsck() { $STOCK ls --json "$1" | jq -s -c '[.[] | select(.message_type=="node")] | {entries: length, logical_bytes: (map(select(.type=="file") | .size) | add)}'; }
log "verifier view $M: $(lsck "$M") receipt $(jq -c '.last.head | {entries, logical_bytes}' "$R/mergebig.json")"
$STOCK check --read-data >"$R/check.txt" 2>&1 && log "stock check --read-data: OK" || log "stock check --read-data FAILED"
rm -rf "$W/tree" "$W/cache"
log done
