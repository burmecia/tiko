#!/usr/bin/env bash
# run_test.sh — tikoblk integration test (this host, ublk2_drv, sudo).
#
#   sudo scripts/tikoblk/run_test.sh
#
# Section 1 (file backend): create 256MB volume -> attach -> mkfs.ext4 ->
# mount -> write file + sha256 -> unmount -> detach -> attach (same device
# node) -> verify checksum -> SIGTERM daemon -> restart -> recovery sweep
# reattaches -> verify checksum -> DELETE -> verify nothing left behind.
#
# Section 2 (chunk backend, store under target/tmp): create 256 MiB chunk
# volume (cache 64 MiB) -> attach -> mkfs.ext4 -> mount -> write 64 MiB
# random + sha256 + sync -> unmount -> SIGTERM -> restart (recovery sweep +
# journal replay) -> mount -> checksum OK -> drain stats check ->
# drop_caches + remount -> checksum OK (chunkstore read path) -> DELETE ->
# store/journal dirs gone.
#
# Section 3 (S3 Files smoke, skipped unless /mnt/s3files is mounted):
# same flow at 64 MiB volume + 16 MiB data with an fsync-heavy write
# pattern, store root /mnt/s3files/tikoblk/smoke-<pid>; then a Phase-3
# addendum: NFS lease conflict (two daemons sharing the store), snapshot +
# clone checksum, POST /gc on the mount, full cleanup.
#
# Section 4 (snapshots/clones/GC, local store): P1 -> snapshot -> P2,
# zero-copy clone reads P1 while origin reads P2, 409-on-delete-with-
# snapshots, POST /gc reclaims everything but the clone's chunks, clone
# survives origin deletion.
#
# Section 5 (single-attach lease, local store): second daemon (own data
# dir, same store root, copied registry) attach -> 409; after daemon1
# detaches, daemon2 attach succeeds.
#
# Re-runnable: cleans up its own leftovers (scratch dir is only ever
# target/tmp/tikoblk-test; only devices registered in its own registry are
# touched, and only via the daemon's control API).
set -euo pipefail

if [[ $(id -u) -ne 0 ]]; then
    exec sudo -E bash "$0" "$@"
fi

# sudo's secure_path drops the dev user's cargo; find it explicitly.
if ! command -v cargo >/dev/null 2>&1; then
    for d in ${SUDO_USER:+/home/$SUDO_USER/.cargo/bin} "$HOME/.cargo/bin" /usr/local/cargo/bin; do
        if [[ -n $d && -x $d/cargo ]]; then
            export PATH="$d:$PATH"
            break
        fi
    done
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
SCRATCH=$REPO_ROOT/target/tmp/tikoblk-test
DATA=$SCRATCH/data
SOCK=$SCRATCH/daemon.sock
MNT=$SCRATCH/mnt
LOG=$SCRATCH/daemon.log
BIN=$REPO_ROOT/target/debug/tikoblkd
VOL=t1
SUMS=$SCRATCH/blob.sha256
STORE=$SCRATCH/store
CACHE_MB=512
SMOKE_STORE=""

DPID=""
DEV=""
STEP="init"

log()  { echo "[run_test] $*"; }
fail() { echo "[run_test] FAIL ($STEP): $*" >&2; exit 1; }

cleanup() {
    local rc=$?
    set +e
    if [[ $rc -ne 0 ]]; then
        echo "[run_test] --- last daemon log lines ---" >&2
        tail -n 30 "$LOG" 2>/dev/null >&2
    fi
    mountpoint -q "$MNT" 2>/dev/null && umount "$MNT"
    if [[ -n $DPID ]] && kill -0 "$DPID" 2>/dev/null; then
        # Best-effort delete of the test volume, then graceful stop (with a
        # SIGKILL fallback; safe here only because I/O is quiesced).
        curl -s --unix-socket "$SOCK" -X DELETE "http://localhost/volumes/$VOL" >/dev/null 2>&1
        kill -TERM "$DPID" 2>/dev/null
        for _ in $(seq 1 100); do
            kill -0 "$DPID" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$DPID" 2>/dev/null
        wait "$DPID" 2>/dev/null
    fi
    # A failed S3 smoke may leave its store dir behind; remove only ours.
    if [[ -n $SMOKE_STORE ]] && mountpoint -q /mnt/s3files 2>/dev/null; then
        rm -rf "$SMOKE_STORE"
    fi
    exit "$rc"
}
trap cleanup EXIT

# --- helpers ---------------------------------------------------------------

api() { # api METHOD PATH [BODY] -> "CODE\nbody"; code on first line
    local method=$1 path=$2 body=${3:-}
    local args=(-s --unix-socket "$SOCK" -X "$method" -w $'\n%{http_code}')
    if [[ -n $body ]]; then
        args+=(-H 'Content-Type: application/json' -d "$body")
    fi
    # body first, then status code (print code first for easy reading)
    local out
    out=$(curl "${args[@]}" "http://localhost$path") || return 1
    local code=${out##*$'\n'}
    local payload=${out%$'\n'*}
    printf '%s\n%s\n' "$code" "$payload"
}

expect() { # expect CODE METHOD PATH [BODY] -> body on stdout
    local want=$1; shift
    local out code
    out=$(api "$@") || fail "curl failed for $1 $2"
    code=${out%%$'\n'*}
    [[ $code == "$want" ]] || fail "$1 $2 -> HTTP $code (want $want): ${out#*$'\n'}"
    [[ $code == "${out}" ]] || printf '%s\n' "${out#*$'\n'}"
    return 0
}

json_get() { python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

start_daemon() {
    STEP="daemon start"
    "$BIN" --ctrl "$CTRL" --data-dir "$DATA" --sock "$SOCK" \
        --store-root "$STORE" --cache-mb "$CACHE_MB" --gc-grace-secs "${GC_GRACE:-600}" \
        --foreground \
        >>"$LOG" 2>&1 &
    DPID=$!
    for _ in $(seq 1 100); do
        local out
        out=$(api GET /health 2>/dev/null) && [[ ${out%%$'\n'*} == 200 ]] && return 0
        sleep 0.1
    done
    fail "daemon did not become healthy"
}

stop_daemon() {
    STEP="daemon stop"
    [[ -n $DPID ]] || return 0
    kill -TERM "$DPID"
    for _ in $(seq 1 100); do
        kill -0 "$DPID" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 "$DPID" 2>/dev/null; then
        # Only safe because this test quiesces I/O before stopping (nothing
        # mounted, no in-flight I/O — see spike NOTES).
        echo "[run_test] WARNING: daemon ignored SIGTERM for 10s; SIGKILL" >&2
        kill -KILL "$DPID"
    fi
    wait "$DPID" 2>/dev/null || true
    DPID=""
}

wait_bdev() { # wait_bdev PATH present|absent
    local p=$1 mode=$2
    for _ in $(seq 1 100); do
        if [[ $mode == present && -b $p ]] || [[ $mode == absent && ! -e $p ]]; then
            return 0
        fi
        sleep 0.1
    done
    fail "device $p did not become $mode"
}

# --- preconditions -----------------------------------------------------------
STEP="preconditions"
# ublk2 (out-of-tree) when present, else mainline (fixed via build_ublk_fixed.sh)
CTRL=/dev/ublk2-control
[[ -e $CTRL ]] || CTRL=/dev/ublk-control
[[ -e $CTRL ]] || fail "no ublk control device (no ublk driver loaded)"
avail_kb=$(df --output=avail / | tail -1)
(( avail_kb >= 6 * 1024 * 1024 )) || fail "less than 6 GB free on /"
mkdir -p "$DATA" "$MNT"
: > "$LOG"

STEP="build"
(cd "$REPO_ROOT" && cargo build -p tikoblk) || fail "cargo build"

# --- pre-clean (leftovers from a crashed previous run) -----------------------
STEP="pre-clean"
mountpoint -q "$MNT" && umount "$MNT"
pkill -TERM -f "tikoblkd --ctrl $CTRL --data-dir $DATA" 2>/dev/null || true
sleep 0.5
# Leftovers ignoring SIGTERM (e.g. stale-socket wakeup race on old builds)
# are safe to SIGKILL: no devices of ours are served after the TERM grace.
pkill -KILL -f "tikoblkd --ctrl $CTRL --data-dir $DATA" 2>/dev/null || true
if [[ -f $DATA/registry.json ]]; then
    start_daemon   # recovery sweep reattaches leftover devices...
    # ...so this can delete every leftover volume in the registry
    for v in $(expect 200 GET /volumes | python3 -c 'import sys,json; [print(v["vol_id"]) for v in json.load(sys.stdin)]'); do
        api DELETE "/volumes/$v" >/dev/null 2>&1 || true
    done
    stop_daemon
fi

# --- section 1: file backend -------------------------------------------------
STEP="start"
log "section 1: file backend (data dir $DATA)"
STORE=$SCRATCH/store CACHE_MB=512
start_daemon

STEP="create"
expect 201 POST /volumes "{\"vol_id\":\"$VOL\",\"size_mb\":256}" >/dev/null
[[ -f $DATA/backing/$VOL.img ]] || fail "backing file missing"
log "volume created"

STEP="attach"
body=$(expect 200 POST "/volumes/$VOL/attach")
DEV=$(echo "$body" | json_get "['device']")
[[ $DEV == /dev/ublkb* ]] || fail "unexpected device path: $DEV"
[[ $(echo "$body" | json_get "['formatted']") == False ]] || fail "formatted != false"
N=${DEV#/dev/ublkb}
REAL=$(readlink -f "$DEV")   # real node: mainline /dev/ublkbN or ublk2 /dev/ublk2bN
wait_bdev "$DEV" present
log "attached on $DEV"

STEP="mkfs+write"
mkfs.ext4 -q -L t1 "$DEV" || fail "mkfs.ext4"
mount "$DEV" "$MNT" || fail "mount"
dd if=/dev/urandom of="$MNT/blob" bs=1M count=32 status=none || fail "dd"
( cd "$MNT" && sha256sum blob > "$SUMS" )
sync
umount "$MNT"
log "wrote 32 MiB, sha256 recorded"

STEP="detach"
expect 200 POST "/volumes/$VOL/detach" >/dev/null
wait_bdev "$DEV" absent
wait_bdev "$REAL" absent
log "detached; device nodes gone"

STEP="reattach same node"
body=$(expect 200 POST "/volumes/$VOL/attach")
DEV2=$(echo "$body" | json_get "['device']")
[[ $DEV2 == "$DEV" ]] || fail "reattach gave $DEV2, want $DEV"
wait_bdev "$DEV" present
sync; echo 3 > /proc/sys/vm/drop_caches   # verify against the device, not page cache
mount "$DEV" "$MNT"
( cd "$MNT" && sha256sum -c "$SUMS" ) || fail "checksum mismatch after reattach"
umount "$MNT"
log "reattach checksum OK"

STEP="SIGTERM + recovery sweep"
stop_daemon
# Device must NOT be deleted: USER_RECOVERY quiesces it for the next daemon.
[[ -e $REAL ]] || fail "device $REAL vanished on daemon exit"
log "daemon stopped; device quiesced"

start_daemon
# The recovery sweep reattaches the volume (state=attached in registry).
body=$(expect 200 GET "/volumes/$VOL")
[[ $(echo "$body" | json_get "['state']") == attached ]] || fail "volume not attached after sweep"
[[ $(echo "$body" | json_get "['dev_id']") == "$N" ]] || fail "dev_id changed across restart"
wait_bdev "$DEV" present
sync; echo 3 > /proc/sys/vm/drop_caches
mount "$DEV" "$MNT"
( cd "$MNT" && sha256sum -c "$SUMS" ) || fail "checksum mismatch after daemon restart"
umount "$MNT"
log "recovery sweep checksum OK"

STEP="delete"
expect 200 DELETE "/volumes/$VOL" >/dev/null
out=$(api GET "/volumes/$VOL"); [[ ${out%%$'\n'*} == 404 ]] || fail "volume still present after delete"
wait_bdev "$DEV" absent
wait_bdev "$REAL" absent
[[ ! -e $DATA/backing/$VOL.img ]] || fail "backing file left behind"
log "deleted; no leftovers"

STEP="stop"
stop_daemon
log "section 1 PASS"

# --- section 2: chunk backend (local store) ----------------------------------
STEP="chunk: start"
log "section 2: chunk backend (store under target/tmp, cache 64 MiB)"
VOL=c1
STORE=$SCRATCH/store-chunk
CACHE_MB=64
start_daemon

STEP="chunk: create"
expect 201 POST /volumes "{\"vol_id\":\"$VOL\",\"size_mb\":256,\"backend\":\"chunk\",\"chunk_size_kib\":1024}" >/dev/null
[[ -f $STORE/volumes/$VOL/map ]] || fail "chunkstore map missing"
log "chunk volume created"

STEP="chunk: attach"
body=$(expect 200 POST "/volumes/$VOL/attach")
DEV=$(echo "$body" | json_get "['device']")
[[ $DEV == /dev/ublkb* ]] || fail "unexpected device path: $DEV"
[[ $(echo "$body" | json_get "['formatted']") == False ]] || fail "formatted != false on fresh chunk volume"
N=${DEV#/dev/ublkb}
wait_bdev "$DEV" present
log "chunk volume attached on $DEV"

STEP="chunk: mkfs+write"
mkfs.ext4 -q -L c1 "$DEV" || fail "mkfs.ext4"
mount "$DEV" "$MNT" || fail "mount"
dd if=/dev/urandom of="$MNT/blob" bs=1M count=64 status=none || fail "dd"
( cd "$MNT" && sha256sum blob > "$SUMS" )
sync
umount "$MNT"
log "wrote 64 MiB, sha256 recorded"

STEP="chunk: SIGTERM + journal replay"
stop_daemon
start_daemon
body=$(expect 200 GET "/volumes/$VOL")
[[ $(echo "$body" | json_get "['state']") == attached ]] || fail "volume not attached after sweep"
wait_bdev "$DEV" present
mount "$DEV" "$MNT"
( cd "$MNT" && sha256sum -c "$SUMS" ) || fail "checksum mismatch after journal replay"
umount "$MNT"
log "journal replay checksum OK"

STEP="chunk: drain + chunkstore read path"
# Wait for the flusher to fold everything into the chunkstore, then prove
# the read path comes from chunk files (cold daemon cache + dropped page
# cache).
for _ in $(seq 1 200); do
    body=$(expect 200 GET "/volumes/$VOL")
    [[ $(echo "$body" | json_get "['stats']['dirty_chunks']") == 0 ]] && break
    sleep 0.2
done
[[ $(echo "$body" | json_get "['stats']['dirty_chunks']") == 0 ]] || fail "flusher did not drain"
[[ $(echo "$body" | json_get "['has_data']") == True ]] || fail "has_data not set after writes"
sync; echo 3 > /proc/sys/vm/drop_caches
mount "$DEV" "$MNT"
( cd "$MNT" && sha256sum -c "$SUMS" ) || fail "checksum mismatch on chunkstore read path"
umount "$MNT"
log "chunkstore read-path checksum OK"

STEP="chunk: delete"
expect 200 DELETE "/volumes/$VOL" >/dev/null
wait_bdev "$DEV" absent
[[ ! -e $STORE/volumes/$VOL ]] || fail "chunkstore volume dir left behind"
[[ ! -e $DATA/journal/$VOL ]] || fail "NVMe journal dir left behind"
log "deleted; store/journal clean"
stop_daemon
log "section 2 PASS"

# --- section 4: snapshots, clones, GC (local store) ---------------------------
STEP="snap: start"
log "section 4: snapshots/clones/GC (local store)"
VOL=a
STORE=$SCRATCH/store-chunk   # reuse section 2's store (dead chunks -> GC fodder)
CACHE_MB=64
GC_GRACE=0
start_daemon

STEP="snap: write P1, snapshot, write P2"
expect 201 POST /volumes '{"vol_id":"a","size_mb":16,"backend":"chunk"}' >/dev/null
body=$(expect 200 POST "/volumes/a/attach")
DEVA=$(echo "$body" | json_get "['device']")
wait_bdev "$DEVA" present
head -c 4M /dev/urandom > "$SCRATCH/p1.bin"
head -c 4M /dev/urandom > "$SCRATCH/p2.bin"
dd if="$SCRATCH/p1.bin" of="$DEVA" bs=1M conv=fsync status=none || fail "write P1"
P1SUM=$(sha256sum "$SCRATCH/p1.bin" | cut -d' ' -f1)
P2SUM=$(sha256sum "$SCRATCH/p2.bin" | cut -d' ' -f1)
body=$(expect 201 POST "/volumes/a/snapshots" '{"name":"s1"}')
SNAP=$(echo "$body" | json_get "['snap_id']")
[[ $SNAP == s1 ]] || fail "unexpected snap_id $SNAP"
[[ -f $STORE/volumes/a/snapshots/s1/map ]] || fail "snapshot map missing"
dd if="$SCRATCH/p2.bin" of="$DEVA" bs=1M conv=fsync status=none || fail "write P2"
log "snapshot s1 taken between P1 and P2"

STEP="snap: zero-copy clone"
expect 201 POST /volumes '{"vol_id":"b","backend":"chunk","from_snapshot":"a/s1"}' >/dev/null
body=$(expect 200 POST "/volumes/b/attach")
DEVB=$(echo "$body" | json_get "['device']")
[[ $(echo "$body" | json_get "['formatted']") == True ]] || fail "clone should report formatted=true"
wait_bdev "$DEVB" present
[[ $(dd if="$DEVB" bs=1M count=4 status=none | sha256sum | cut -d' ' -f1) == "$P1SUM" ]] \
    || fail "clone does not read snapshot point P1"
[[ $(dd if="$DEVA" bs=1M count=4 status=none | sha256sum | cut -d' ' -f1) == "$P2SUM" ]] \
    || fail "origin does not read P2"
log "clone reads P1, origin reads P2"

STEP="snap: delete protections"
expect 409 DELETE "/volumes/a" >/dev/null
expect 200 GET "/volumes/a/snapshots" >/dev/null
expect 200 DELETE "/volumes/a/snapshots/s1" >/dev/null
expect 200 POST "/volumes/a/detach" >/dev/null
expect 200 DELETE "/volumes/a" >/dev/null
log "409-with-snapshots, snapshot delete, volume delete OK"

STEP="gc: mark-and-sweep"
# Quiesce the flusher so grace=0 cannot race an in-flight chunk write.
sleep 2
BEFORE=$(find "$STORE/chunks" -type f ! -name '*.tmp' | wc -l)
body=$(expect 200 POST /gc)
RECLAIMED=$(echo "$body" | json_get "['reclaimed_count']")
AFTER=$(find "$STORE/chunks" -type f ! -name '*.tmp' | wc -l)
EXPECTED=$(python3 -c "
data = open('$STORE/volumes/b/map','rb').read()
ids = set()
for off in range(64, len(data), 16):
    i = data[off:off+16]
    if any(i): ids.add(i)
print(len(ids))")
[[ $RECLAIMED -gt 0 ]] || fail "gc reclaimed nothing (pool had $BEFORE)"
[[ $AFTER == "$EXPECTED" ]] || fail "pool after gc: $AFTER files, want $EXPECTED (clone's chunks)"
log "gc: $BEFORE -> $AFTER pool chunks (reclaimed $RECLAIMED)"

STEP="gc: clone survives origin delete"
[[ $(dd if="$DEVB" bs=1M count=4 status=none | sha256sum | cut -d' ' -f1) == "$P1SUM" ]] \
    || fail "clone unreadable after origin delete + gc"
expect 200 POST "/volumes/b/detach" >/dev/null
expect 200 DELETE "/volumes/b" >/dev/null
sleep 2
expect 200 POST /gc >/dev/null
[[ $(find "$STORE/chunks" -type f ! -name '*.tmp' | wc -l) == 0 ]] || fail "pool not empty after final gc"
log "clone survived origin delete; pool fully reclaimed"
stop_daemon
log "section 4 PASS"

# --- section 5: single-attach lease, second daemon (local store) --------------
STEP="lease: start"
log "section 5: lease conflict with a second daemon"
VOL=l1
STORE=$SCRATCH/store-lease
CACHE_MB=64
GC_GRACE=600
start_daemon
DPID_MAIN=$DPID   # daemon1's pid: DPID is reused by daemon2 starts below
expect 201 POST /volumes '{"vol_id":"l1","size_mb":16,"backend":"chunk"}' >/dev/null
expect 200 POST "/volumes/l1/attach" >/dev/null

# Second daemon with its own data dir but the same store root. Its registry
# gets a copy of daemon1's (two-hosts-know-the-same-volume simulation).
DATA2=$SCRATCH/data2
SOCK2=$SCRATCH/daemon2.sock
mkdir -p "$DATA2"
cp "$DATA/registry.json" "$DATA2/registry.json"
DATA_SAVE=$DATA; SOCK_SAVE=$SOCK
DATA=$DATA2; SOCK=$SOCK2
start_daemon
out=$(api POST "/volumes/l1/attach")
[[ ${out%%$'\n'*} == 409 ]] || fail "second daemon attach should 409 (lease), got: $out"
echo "$out" | grep -qi 'already attached' || fail "409 body lacks 'already attached': $out"
log "lease conflict over shared store: 409 as required"
stop_daemon   # stop daemon2

# After daemon1 detaches, daemon2 may attach.
DATA=$DATA_SAVE; SOCK=$SOCK_SAVE
expect 200 POST "/volumes/l1/detach" >/dev/null
DATA=$DATA2; SOCK=$SOCK2
start_daemon
expect 200 POST "/volumes/l1/attach" >/dev/null
expect 200 POST "/volumes/l1/detach" >/dev/null
stop_daemon
DATA=$DATA_SAVE; SOCK=$SOCK_SAVE
expect 200 DELETE "/volumes/l1" >/dev/null
DPID=$DPID_MAIN   # back to daemon1 for the final stop
stop_daemon
log "section 5 PASS"

# --- section 3: S3 Files smoke ------------------------------------------------
if mountpoint -q /mnt/s3files; then
    STEP="s3: start"
    log "section 3: S3 Files smoke (/mnt/s3files)"
    VOL=s1
    SMOKE_STORE=/mnt/s3files/tikoblk/smoke-$$
    STORE=$SMOKE_STORE
    CACHE_MB=64
    GC_GRACE=0   # gc runs here are quiesced; grace would leave dead chunks
    start_daemon

    STEP="s3: create+attach"
    expect 201 POST /volumes "{\"vol_id\":\"$VOL\",\"size_mb\":64,\"backend\":\"chunk\"}" >/dev/null
    body=$(expect 200 POST "/volumes/$VOL/attach")
    DEV=$(echo "$body" | json_get "['device']")
    N=${DEV#/dev/ublkb}
    wait_bdev "$DEV" present

    STEP="s3: mkfs+fsync-heavy write"
    mkfs.ext4 -q -L s1 "$DEV" || fail "mkfs.ext4"
    mount "$DEV" "$MNT" || fail "mount"
    for i in $(seq 1 16); do
        dd if=/dev/urandom of="$MNT/blob.$i" bs=1M count=1 conv=fsync status=none || fail "dd fsync $i"
    done
    ( cd "$MNT" && sha256sum blob.* > "$SUMS" )
    umount "$MNT"
    log "wrote 16 MiB in 16 fsync'd files"

    STEP="s3: SIGTERM + replay + checksum"
    stop_daemon
    start_daemon
    wait_bdev "$DEV" present
    mount "$DEV" "$MNT"
    ( cd "$MNT" && sha256sum -c "$SUMS" ) || fail "checksum mismatch after restart"
    umount "$MNT"
    for _ in $(seq 1 200); do
        body=$(expect 200 GET "/volumes/$VOL")
        [[ $(echo "$body" | json_get "['stats']['dirty_chunks']") == 0 ]] && break
        sleep 0.2
    done
    sync; echo 3 > /proc/sys/vm/drop_caches
    mount "$DEV" "$MNT"
    ( cd "$MNT" && sha256sum -c "$SUMS" ) || fail "checksum mismatch on S3 Files read path"
    umount "$MNT"
    log "S3 Files checksums OK"

    STEP="s3: delete"
    expect 200 DELETE "/volumes/$VOL" >/dev/null
    wait_bdev "$DEV" absent
    [[ ! -e $SMOKE_STORE/volumes/$VOL ]] || fail "S3 store volume dir left behind"
    log "deleted; S3 Files store clean"

    STEP="s3: lease conflict over NFS"
    # Two daemon instances sharing the S3 Files store root: the flock'd
    # map.lock must actually conflict over NFSv4 (the two-host premise).
    expect 201 POST /volumes '{"vol_id":"n1","size_mb":16,"backend":"chunk"}' >/dev/null
    expect 200 POST "/volumes/n1/attach" >/dev/null
    DATA2=$SCRATCH/data2-nfs
    SOCK2=$SCRATCH/daemon2-nfs.sock
    mkdir -p "$DATA2"
    cp "$DATA/registry.json" "$DATA2/registry.json"
    DATA_SAVE=$DATA; SOCK_SAVE=$SOCK
    DATA=$DATA2; SOCK=$SOCK2
    start_daemon
    out=$(api POST "/volumes/n1/attach")
    [[ ${out%%$'\n'*} == 409 ]] || fail "NFS lease conflict should 409, got: $out"
    echo "$out" | grep -qi 'already attached' || fail "409 body lacks 'already attached': $out"
    log "NFS lease conflict: 409 as required"
    stop_daemon
    DATA=$DATA_SAVE; SOCK=$SOCK_SAVE
    expect 200 POST "/volumes/n1/detach" >/dev/null

    STEP="s3: snapshot + clone + gc over NFS"
    body=$(expect 200 POST "/volumes/n1/attach")
    DEV=$(echo "$body" | json_get "['device']")
    wait_bdev "$DEV" present
    head -c 8M /dev/urandom > "$SCRATCH/np1.bin"
    head -c 8M /dev/urandom > "$SCRATCH/np2.bin"
    NP1=$(sha256sum "$SCRATCH/np1.bin" | cut -d' ' -f1)
    dd if="$SCRATCH/np1.bin" of="$DEV" bs=1M conv=fsync status=none || fail "write P1 (NFS)"
    expect 201 POST "/volumes/n1/snapshots" '{"name":"s1"}' >/dev/null
    dd if="$SCRATCH/np2.bin" of="$DEV" bs=1M conv=fsync status=none || fail "write P2 (NFS)"
    expect 201 POST /volumes '{"vol_id":"n2","backend":"chunk","from_snapshot":"n1/s1"}' >/dev/null
    body=$(expect 200 POST "/volumes/n2/attach")
    DEV2=$(echo "$body" | json_get "['device']")
    wait_bdev "$DEV2" present
    sync; echo 3 > /proc/sys/vm/drop_caches
    [[ $(dd if="$DEV2" bs=1M count=8 status=none | sha256sum | cut -d' ' -f1) == "$NP1" ]] \
        || fail "NFS clone checksum mismatch"
    log "NFS snapshot+clone checksum OK"

    expect 200 POST "/volumes/n2/detach" >/dev/null
    expect 200 DELETE "/volumes/n2" >/dev/null
    expect 200 POST "/volumes/n1/detach" >/dev/null
    expect 409 DELETE "/volumes/n1" >/dev/null
    expect 200 DELETE "/volumes/n1/snapshots/s1" >/dev/null
    expect 200 DELETE "/volumes/n1" >/dev/null
    sleep 2   # let the flusher go idle so grace=0 cannot race it
    body=$(expect 200 POST /gc)
    log "NFS gc: $(echo "$body" | tr -d '\n')"
    [[ $(find "$SMOKE_STORE/chunks" -type f ! -name '*.tmp' 2>/dev/null | wc -l) == 0 ]] \
        || fail "S3 pool not empty after gc"
    # Remove the whole smoke store (empty shard dirs remain after gc).
    rm -rf "$SMOKE_STORE"
    [[ ! -e $SMOKE_STORE ]] || fail "S3 smoke store dir left behind"
    SMOKE_STORE=""
    stop_daemon
    log "section 3 PASS"
else
    log "section 3 SKIPPED: /mnt/s3files not mounted"
fi

rm -rf "$SCRATCH"

echo "[run_test] PASS"
