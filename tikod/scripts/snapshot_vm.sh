#!/bin/bash
#
# Create a full Firecracker snapshot of a running VM.
#
# Flow: pause vCPUs -> write microVM-state + guest-memory files -> resume.
# The VM keeps running (non-disruptive). The guest memory file can be up to
# mem_size_mib (512 MB) and is written under tikod/assets/snapshots/vm-<id>/.
#
# The disk backing file (overlay-<id>.ext4) is NOT part of the snapshot —
# Firecracker only flushes it to the host FS cache (cache_type=Unsafe means the
# guest cannot issue flushes). For a clean save/restore test, snapshot then stop
# the VM immediately so disk state stays close to memory state.
#
# Usage:
#   ./snapshot_vm.sh [VM_ID] [--no-resume]
#       --no-resume   leave the VM paused after creating the snapshot
#
# Env: SNAPSHOT_DIR (default $ASSETS_DIR/snapshots/vm-<VM_ID>)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/../assets"

VM_ID="${VM_ID:-0}"
RESUME=1
while [ $# -gt 0 ]; do
    case "$1" in
        --no-resume) RESUME=0; shift ;;
        *) VM_ID="$1"; shift ;;
    esac
done

if ! [[ "$VM_ID" =~ ^[0-9]+$ ]]; then
    echo "VM_ID must be a non-negative integer (got '$VM_ID')" >&2
    exit 1
fi

API_SOCKET="/tmp/fc-${VM_ID}.socket"
SNAPSHOT_DIR="${SNAPSHOT_DIR:-$ASSETS_DIR/snapshots/vm-${VM_ID}}"
STATE_FILE="$SNAPSHOT_DIR/snap.bin"
MEM_FILE="$SNAPSHOT_DIR/mem.bin"

if [ ! -S "$API_SOCKET" ]; then
    echo "no API socket at $API_SOCKET — is VM ${VM_ID} running?" >&2
    exit 1
fi
if ! pgrep -f -- "--api-sock $API_SOCKET" >/dev/null; then
    echo "no live firecracker for $API_SOCKET (stale socket?)" >&2
    exit 1
fi

mkdir -p "$SNAPSHOT_DIR"
STATE_ABS="$(cd "$SNAPSHOT_DIR" && pwd)/snap.bin"
MEM_ABS="$(cd "$SNAPSHOT_DIR" && pwd)/mem.bin"

# Firecracker API helper: method path body -> fails the script on non-2xx.
fc_api() {
    local method="$1" path="$2" body="${3:-}" code body_file
    body_file="$(mktemp)"
    # Firecracker runs (and creates its API socket) as root via sudo; the socket
    # is root-owned mode 0755, and connect() needs write perms -> use sudo curl.
    # `|| true`: curl occasionally exits non-zero (write error) on unix-socket
    # teardown even on a 204; we rely on the captured http_code, not the exit.
    if [ -n "$body" ]; then
        code=$(sudo curl -s -o "$body_file" --unix-socket "$API_SOCKET" \
            -X "$method" "http://localhost$path" \
            -H 'Accept: application/json' -H 'Content-Type: application/json' \
            -d "$body" -w "%{http_code}" || true)
    else
        code=$(sudo curl -s -o "$body_file" --unix-socket "$API_SOCKET" \
            -X "$method" "http://localhost$path" -H 'Accept: application/json' -w "%{http_code}" || true)
    fi
    if [ "$code" != "204" ] && [ "$code" != "200" ]; then
        echo "API $method $path -> HTTP $code: $(cat "$body_file")" >&2
        rm -f "$body_file"
        return 1
    fi
    rm -f "$body_file"
}

echo ">>> VM ${VM_ID}: pausing..."
fc_api PATCH /vm '{"state":"Paused"}'
# The 204 guarantees Paused, but give KVM a moment to settle.
sleep 0.5

echo ">>> VM ${VM_ID}: creating full snapshot..."
fc_api PUT /snapshot/create \
    "{\"snapshot_type\":\"Full\",\"snapshot_path\":\"$STATE_ABS\",\"mem_file_path\":\"$MEM_ABS\"}"

if [ "$RESUME" -eq 1 ]; then
    echo ">>> VM ${VM_ID}: resuming..."
    fc_api PATCH /vm '{"state":"Resumed"}'
else
    echo ">>> VM ${VM_ID}: left paused (--no-resume)."
fi

echo ">>> snapshot files:"
ls -lh "$STATE_FILE" "$MEM_FILE" | awk '{print "    "$5"  "$9}'
echo "    mem file must be kept immutable for the lifetime of any VM resumed from it."
