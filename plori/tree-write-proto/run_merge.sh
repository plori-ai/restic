#!/usr/bin/env bash
# Compare fixture for serve-write /merge-write: run_merge.sh local|minio
# Three revisions of a 7-file tree (base, current, incoming) are backed up as
# helper-style heads and public twins; the real merge.Merge (plori-ws-rev,
# through a go test overlay) produces the merged entries and generated diff3
# blobs, and the platform materializer (resticrev.Repository.Write) the
# reference snapshot. serve-write writes the integration snapshot from the
# entries; both are compared node by node and by restore.
set -euo pipefail
: "${PLORI_RUNTIME:?set PLORI_RUNTIME to a plori-runtime checkout}"
S=$(cd "$(dirname "$0")" && pwd)
OUT=${WORK:-/tmp/tree-write-proto}
B=$S/bin
MODE=$1
W=$OUT/work-merge-$MODE
R=$OUT/results/merge-$MODE
rm -rf "$W" "$R"
mkdir -p "$W" "$R"
log() { echo "[$(date +%T)] $*" | tee -a "$R/run.log"; }
export RESTIC_PASSWORD
RESTIC_PASSWORD="$(head -c 24 /dev/urandom | base64)"
export RESTIC_CACHE_DIR=$W/cache
ENDPOINT=192.168.49.2:30902
BUCKET=plori-workspaces-rev-115ba84e4
if [ "$MODE" = minio ]; then
	AWS_ACCESS_KEY_ID="$(command kubectl --context minikube -n plori get secret plori-workspace-object-key -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)"
	AWS_SECRET_ACCESS_KEY="$(command kubectl --context minikube -n plori get secret plori-workspace-object-key -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)"
	export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
	PREFIX=proto-tree-write/$(cat /proc/sys/kernel/random/uuid)
	export RESTIC_REPOSITORY=s3:http://$ENDPOINT/$BUCKET/$PREFIX
	log "minio prefix $PREFIX"
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
STOCK=$B/restic-stock
$STOCK init -q
umask 022
mk() { mkdir -p "$(dirname "$1")"; printf '%b' "$2" >"$1"; }
lines() { for i in $(seq 1 12); do if [ "$i" = "$1" ]; then echo "line $i changed by $2"; else echo "line $i"; fi; done; }
for side in base current incoming; do
	d=$W/$side
	mkdir -p "$d"
	mk "$d/README.md" '# Project\n'
	mkdir -p "$d/src"
	lines 0 none >"$d/src/main.py"
	mk "$d/src/util.py" 'def util():\n    pass\n'
	mkdir -p "$d/data"
	head -c 4096 /dev/zero | tr '\0' 'b' >"$d/data/blob.bin"
	mk "$d/docs/a.md" 'doc a\n'
	mk "$d/assets/x" 'asset v1\n'
	ln "$d/assets/x" "$d/assets/y"
	ln -s src/main.py "$d/sym"
	mk "$d/.forge/state" 'private state\n'
done
lines 2 current >"$W/current/src/main.py"
mk "$W/current/notes.txt" 'notes from current\n'
lines 11 incoming >"$W/incoming/src/main.py"
mk "$W/incoming/docs/a.md" 'doc a, revised by the worker\n'
rm "$W/incoming/src/util.py"
chmod 0755 "$W/incoming/data/blob.bin"
mk "$W/incoming/new/file.txt" 'new from incoming\n'
printf 'asset v2\n' >"$W/incoming/assets/x"
EXCL=(--iexclude lost+found --iexclude '.nfs*' --iexclude .forge --iexclude .trash --iexclude .plori-trash --iexclude .plori-workspace
	--iexclude .control --iexclude .config --iexclude .jfs --iexclude .stats --iexclude .accesslog)
backup() {
	local dir=$1 parent=$2 public=$3 args=(backup --json --host plori-workspace --tag "plori-op:$4" --tag plori-copy:proto-copy)
	[ "$public" = 1 ] && args+=(--tag plori-public "${EXCL[@]}")
	[ -n "$parent" ] && args+=(--parent "$parent")
	(cd "$dir" && $STOCK "${args[@]}" . | jq -r 'select(.message_type=="summary") | .snapshot_id')
}
declare -A PUB
for side in base current incoming; do
	h=$(backup "$W/$side" "" 0 "$side")
	PUB[$side]=$(backup "$W/$side" "$h" 1 "$side")
done
log "revisions (public twins): base ${PUB[base]} current ${PUB[current]} incoming ${PUB[incoming]}"
t0=$(date +%s.%N)
P=$PLORI_RUNTIME/services/storage-worker/internal/workspacerev/resticrev
printf '{"Replace":{"%s/zz_proto_merge_test.go":"%s/overlay/proto_merge_test.go"}}\n' "$P" "$S" >"$W/overlay.json"
(cd "$PLORI_RUNTIME" && \
	PROTO_REPO=$RESTIC_REPOSITORY PROTO_BIN=$B/restic-fork PROTO_BASE=${PUB[base]} PROTO_CURRENT=${PUB[current]} PROTO_INCOMING=${PUB[incoming]} PROTO_OUT=$W/merged.json \
	go test -overlay "$W/overlay.json" -run TestProtoMergeFixture -count=1 -v ./services/storage-worker/internal/workspacerev/resticrev/ 2>&1 | grep -E "merged|FAIL|ok|Error" | tee -a "$R/run.log")
log "merge + platform materializer reference: $(echo "$(date +%s.%N) - $t0" | bc) s (includes go test build)"
REF=$(cat "$W/merged.json.ref")
jq -c '{entries: (.entries|length), blobs: (.blobs|length), changes: (.changes|length), conflicts: (.conflicts|length)}' "$W/merged.json" | tee -a "$R/run.log"
cp "$W/merged.json" "$R/merged.json"
SOCK=/tmp/bproto-merge-$MODE.sock
rm -f "$SOCK"
$B/restic-fork serve-write --socket "$SOCK" 2>"$R/serve-write.stderr" &
SWPID=$!
for _ in $(seq 1 600); do [ -S "$SOCK" ] && break; sleep 0.1; done
log "merge-write: $($B/driver merge -socket "$SOCK" -manifest "$W/merged.json" -base "${PUB[current]}" -a "${PUB[base]}" -b "${PUB[incoming]}" -n 20 -out "$R/merge.json")"
H=$(jq -r .last.head.snapshot "$R/merge.json")
log "compare tree-write $H vs platform reference $REF: $($B/driver compare -socket "$SOCK" -a "$H" -b "$REF" | tee "$R/compare.txt" | tail -1)"
kill "$SWPID"
wait "$SWPID" || true
SWPID=""
rm -rf "$W/ra" "$W/rb"
$STOCK restore -q "$H" --target "$W/ra"
$STOCK restore -q "$REF" --target "$W/rb"
log "restore compare: $(python3 "$S/compare_restore.py" "$W/ra" "$W/rb" | tee "$R/restore-compare.txt" | tail -1)"
(cd "$W/ra" && find . -printf '%y %m %n %p %l\n' | sort) >"$R/tree-write-listing.txt"
$STOCK check --read-data >"$R/check.txt" 2>&1 && log "stock check --read-data: OK" || { log "stock check --read-data FAILED"; tail -5 "$R/check.txt"; }
log done
