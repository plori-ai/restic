#!/usr/bin/env bash
# Prototype run for serve-write: run.sh local|minio ENTRIES
# Generates a tree, writes a helper-style head and public twin with stock
# restic, measures chained serve-write edits, then builds reference snapshots
# by applying the same edits to the directory and backing up with stock
# restic, compares node by node and by restore, and runs check --read-data.
# Needs bin/restic-stock (restic v0.19.1), bin/restic-fork (this branch),
# bin/driver (driver/), bin/s3tool (s3tool/, MinIO runs only), python3, jq, bc.
# Work files and results go to $WORK (default /tmp/tree-write-proto).
set -euo pipefail
S=$(cd "$(dirname "$0")" && pwd)
OUT=${WORK:-/tmp/tree-write-proto}
B=$S/bin
MODE=$1
N=$2
W=$OUT/work-$MODE-$N
R=$OUT/results/$MODE-$N
rm -rf "$W" "$R"
mkdir -p "$W" "$R"
TREE=$W/tree
log() { echo "[$(date +%T)] $*" | tee -a "$R/run.log"; }

python3 "$S/gen_tree.py" "$TREE" "$N" 42 | tee "$R/tree.json"
export RESTIC_PASSWORD
RESTIC_PASSWORD="$(head -c 24 /dev/urandom | base64)"
export RESTIC_CACHE_DIR=$W/cache
ENDPOINT=192.168.49.2:30902
BUCKET=plori-workspaces-rev-115ba84e4
PREFIX=""
if [ "$MODE" = minio ]; then
	AWS_ACCESS_KEY_ID="$(command kubectl --context minikube -n plori get secret plori-workspace-object-key -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)"
	AWS_SECRET_ACCESS_KEY="$(command kubectl --context minikube -n plori get secret plori-workspace-object-key -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)"
	export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
	PREFIX=proto-tree-write/$(cat /proc/sys/kernel/random/uuid)
	export RESTIC_REPOSITORY=s3:http://$ENDPOINT/$BUCKET/$PREFIX
	log "minio prefix $PREFIX (credential lengths ${#AWS_ACCESS_KEY_ID} ${#AWS_SECRET_ACCESS_KEY})"
	cleanup() {
		[ -n "${SWPID:-}" ] && kill "$SWPID" 2>/dev/null && wait "$SWPID" 2>/dev/null || true
		"$B/s3tool" delete "$ENDPOINT" "$BUCKET" "$PREFIX" | tee -a "$R/run.log"
		"$B/s3tool" size "$ENDPOINT" "$BUCKET" "$PREFIX" | tee -a "$R/run.log"
	}
else
	export RESTIC_REPOSITORY=$W/repo
	cleanup() { [ -n "${SWPID:-}" ] && kill "$SWPID" 2>/dev/null && wait "$SWPID" 2>/dev/null || true; }
fi
trap cleanup EXIT
repo_bytes() {
	if [ "$MODE" = minio ]; then "$B/s3tool" size "$ENDPOINT" "$BUCKET" "$PREFIX" | jq .bytes; else du -sb "$W/repo" | cut -f1; fi
}
STOCK=$B/restic-stock
$STOCK init -q
EXCL=(--iexclude lost+found --iexclude '.nfs*' --iexclude .forge --iexclude .trash --iexclude .plori-trash --iexclude .plori-workspace
	--iexclude .control --iexclude .config --iexclude .jfs --iexclude .stats --iexclude .accesslog)
# backup DIR PARENT TAG... prints the snapshot ID (the helper's resticBackup arguments).
backup() {
	local dir=$1 parent=$2 public=$3 args=(backup --json --host plori-workspace --tag "plori-op:$4" --tag plori-copy:proto-copy)
	[ "$public" = 1 ] && args+=(--tag plori-public "${EXCL[@]}")
	[ -n "$parent" ] && args+=(--parent "$parent")
	(cd "$dir" && $STOCK "${args[@]}" . | jq -r 'select(.message_type=="summary") | .snapshot_id')
}
t0=$(date +%s.%N)
BASE=$(backup "$TREE" "" 0 base)
t1=$(date +%s.%N)
BASEP=$(backup "$TREE" "$BASE" 1 base)
t2=$(date +%s.%N)
log "base head $BASE ($(echo "$t1 - $t0" | bc) s) public $BASEP ($(echo "$t2 - $t1" | bc) s)"

SOCK=/tmp/bproto-$MODE-$N.sock
rm -f "$SOCK"
$B/restic-fork serve-write --socket "$SOCK" 2>"$R/serve-write.stderr" &
SWPID=$!
for _ in $(seq 1 600); do [ -S "$SOCK" ] && break; sleep 0.1; done
D=$B/driver
$D init -state "$W/state.json" -manifest "$TREE.manifest.json" -base "$BASE"
log "prepare cold: $($D prepare -socket "$SOCK" -base "$BASE")"
log "prepare again: $($D prepare -socket "$SOCK" -base "$BASE")"
G0=$(repo_bytes)
for sc in "overwrite 30" "rename 20" "pair 10"; do
	set -- $sc
	g=$(repo_bytes)
	log "$($D run -socket "$SOCK" -state "$W/state.json" -scenario "$1" -n "$2" -log "$W/edits.jsonl" -out "$R/$1.json")"
	log "repository bytes added by $1: $(( $(repo_bytes) - g )) for $2 edits"
done
H3=$(jq -r .last.head.snapshot "$R/pair.json")
T3=$(jq -r .last.public.snapshot "$R/pair.json")
g=$(repo_bytes)
log "$($D run -socket "$SOCK" -state "$W/state.json" -scenario trash -n 11 -log "$W/edits.jsonl" -out "$R/trash.json")"
log "repository bytes added by trash: $(( $(repo_bytes) - g )) for 11 edits"
H4=$(jq -r .last.head.snapshot "$R/trash.json")

# The verifier's view (restic ls --json node count and file bytes) against the receipts.
lsck() { $STOCK ls --json "$1" | jq -s -c '[.[] | select(.message_type=="node")] | {entries: length, logical_bytes: (map(select(.type=="file") | .size) | add)}'; }
log "verifier view head $H3: $(lsck "$H3") receipt $(jq -c '.last.head | {entries, logical_bytes}' "$R/pair.json")"
log "verifier view twin $T3: $(lsck "$T3") receipt $(jq -c '.last.public | {entries, logical_bytes}' "$R/pair.json")"
log "verifier view head $H4: $(lsck "$H4") receipt $(jq -c '.last.head | {entries, logical_bytes}' "$R/trash.json")"
# Reference: the same edits on the directory, backed up like the helper.
$D apply -dir "$TREE" -log "$W/edits.jsonl" -from 0 -to 60
R3=$(backup "$TREE" "$BASE" 0 ref3)
R3P=$(backup "$TREE" "$R3" 1 ref3)
$D apply -dir "$TREE" -log "$W/edits.jsonl" -from 60
R4=$(backup "$TREE" "$R3" 0 ref4)
log "reference heads $R3 (twin $R3P) $R4"
log "compare head after pair (tree-write $H3 vs reference $R3): $($D compare -socket "$SOCK" -a "$H3" -b "$R3" | tail -1)"
log "compare twin after pair (tree-write $T3 vs reference $R3P): $($D compare -socket "$SOCK" -a "$T3" -b "$R3P" | tail -1)"
log "compare head after trash (tree-write $H4 vs reference $R4): $($D compare -socket "$SOCK" -a "$H4" -b "$R4" | tail -1)"
$D compare -socket "$SOCK" -a "$H4" -b "$R4" >"$R/compare-h4.txt" || true
kill "$SWPID"
wait "$SWPID" || true
SWPID=""
for pair in "$H3 $R3" "$T3 $R3P" "$H4 $R4"; do
	set -- $pair
	rm -rf "$W/ra" "$W/rb"
	$STOCK restore -q "$1" --target "$W/ra"
	$STOCK restore -q "$2" --target "$W/rb"
	log "restore compare $1 vs $2: $(python3 "$S/compare_restore.py" "$W/ra" "$W/rb" | tail -1)"
done
rm -rf "$W/ra" "$W/rb"
t0=$(date +%s.%N)
$STOCK check --read-data >"$R/check.txt" 2>&1 && log "stock check --read-data: OK ($(echo "$(date +%s.%N) - $t0" | bc) s)" || { log "stock check --read-data FAILED"; tail -5 "$R/check.txt"; }
$STOCK snapshots --json | jq -c '[.[] | {id: .short_id, parent: (.parent // "" | .[0:8]), tags}]' >"$R/snapshots.json"
log "snapshots: $(jq length "$R/snapshots.json")"
rm -rf "$TREE" "$W/cache"
log done
