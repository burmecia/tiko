# Plan: custom ublk block daemon (`tikoblk`) for Tiko's `remote_slow` tier

## Goal

A host-side userspace block daemon that serves per-VM persistent volumes as
`/dev/ublkbN` devices, backed by **immutable 1 MiB chunks stored file-per-chunk
on a host-mounted S3 Files share**, with a host-NVMe write-back cache/journal.
Firecracker attaches the device node as an ordinary virtio-block drive, so
snapshots/restore keep working (the reason ublk was chosen over
vhost-user-block). Guests see a plain block device: no NFS client, no AWS
credentials.

## Architecture

```
guest VM ──virtio-blk──► Firecracker ──pread/pwrite──► /dev/ublkbN
                                                            │ (kernel ublk, io_uring uring_cmd)
                                                            ▼
                         tikoblkd (Rust, one per host, root)
                          ├─ dirty write buffer (per volume, keyed by chunk idx)
                          ├─ NVMe write journal (durability on FLUSH) + read cache (LRU)
                          └─ chunk store: file-per-chunk on S3 Files mount (NFSv4.2)
tikovm-hostd ──HTTP over UDS (/run/tikoblk/daemon.sock)──► tikoblkd control API
```

Key decisions locked by prior discussion:

- **Chunks are immutable** (write tmp + fsync + rename; new id per write) —
  kills S3 Files version churn, makes ublk `REISSUE` double-writes harmless,
  gives COW snapshots/clones via chunk-map copy, and confines mutability to a
  tiny per-volume map file.
- **Chunk size 1 MiB default** (one max-size NFS op, 800 MiB/s export ceiling
  per FS, 256× max flush amplification absorbed by the cache); per-volume
  parameter (256 KiB–4 MiB) at creation.
- **`UBLK_F_USER_RECOVERY` + `REISSUE`** so daemon restart/upgrade doesn't kill
  VMs; kernel doc explicitly recommends REISSUE for VM backends.
- Host kernel is 6.17 (`CONFIG_BLK_DEV_UBLK=m`); recovery (6.2+), unprivileged
  (6.3+), zero-copy (6.15+) all available. Module package missing → install
  `linux-modules-extra-6.17.0-1019-aws` + `modprobe ublk_drv`.

## Storage layout (on the S3 Files mount, `<source>` = e.g. /mnt/s3files/tikoblk)

```
<source>/volumes/<vol_id>/
    map                 # header{magic,ver,vol_id,size,chunk_size,epoch,generation} + chunk-id array
    map.journal/        # append-only map deltas; folded into `map` on checkpoint
                        #   (same pattern as worker's manifest/segment compactor)
    map.lock            # advisory flock held while attached = single-attach lease
    chunks/ab/cd/<id>   # immutable chunk files (id = 128-bit random), optional zstd
    snapshots/<snap_id>/map   # frozen map copy (COW snapshot/clone)
```

- Reads: dirty buffer → NVMe read cache → chunk file (fetch whole chunk,
  decompress, cache). Writes: coalesce in dirty buffer; on guest FLUSH or dirty
  threshold → append dirty chunk payloads to per-volume NVMe journal segment +
  fsync (one fast sequential write), ack; background task writes chunk files
  (tmp+fsync+rename → NFS COMMIT durability), then appends map delta + fsync,
  then reclaims the journal segment. Data-before-metadata ordering throughout.
- GC: periodic mark-and-sweep — union of chunk ids over all live volume maps +
  snapshot maps vs. files under `chunks/`; delete unreferenced (plus orphan
  `*.tmp`). Required because chunks are immutable.
- Single-attach: flock on `map.lock` + epoch bump in map header; attach fails
  if already held (prevents two hosts/VMs mounting one volume → corruption).
- Fresh volumes: `tikoblkd` reports `formatted=false`; tikovm-host runs
  `mkfs.ext4 -L <name>` on `/dev/ublkbN` before attach (mirrors today's
  `provision_drives`, minimal change).

## New crate: `tikoblk/` (workspace member, binaries `tikoblkd`)

Follows tikovm-* conventions: edition 2024, rust 1.88, tracing/serde/zstd from
workspace deps, **no pgsys/core deps** (clippy-clean like tikovm-*). Layout:

```
tikoblk/src/
  main.rs        # tikoblkd: args, registry load, recovery sweep, control API, systemd notify
  device.rs      # ublk device lifecycle via `libublk` crate (ADD/SET_PARAMS/START/STOP/DEL,
                 #   FETCH→COMMIT loop, 1 queue/device, USER_RECOVERY+REISSUE)
  volume.rs      # volume open/create, dirty buffer, FLUSH handling, mkfs state
  chunkstore.rs  # file-per-chunk IO on the mount (tmp+rename, fsync, fetch, shard dirs)
  map.rs         # map file + map-delta journal + fold (checkpoint)
  cache.rs       # NVMe read cache (LRU) + write journal segments + replay
  gc.rs          # mark-and-sweep
  control.rs     # HTTP-over-UDS API (mirrors firecracker.rs's UDS HTTP style)
  registry.rs    # /var/lib/tikoblk/registry.json: vol→dev_id, state, for recovery
```

Control API (HTTP over `/run/tikoblk/daemon.sock`):
`POST /volumes` {vol_id, size_mb, chunk_size_kib?} • `POST /volumes/{id}/attach`
→ {device: "/dev/ublkbN", formatted} • `POST /volumes/{id}/detach` •
`DELETE /volumes/{id}` • `POST /volumes/{id}/snapshots` •
`POST /volumes/{id}/clones` • `GET /volumes/{id}` • `GET /health` • `GET /metrics`

Ops model: runs as root under `unshare --mount` (ublk mount-namespace
self-deadlock caveat), systemd unit, `/dev/ublk-control` access.

## tikovm integration (phase 4)

- Extract `provision_drives` from `tikovm-host/src/vmm/firecracker.rs:469` into
  `tikovm-host/src/storage/volume.rs` behind a `RemoteBacking` trait (the design
  §9 module that never got built): `provision(&VolumeDecl, vm_id) -> PathBuf`,
  `on_destroy(vm_id)`, `on_suspend(vm_id)`.
- Two impls: `S3FilesImage` (today's truncate+mkfs behavior, moved verbatim) and
  `Ublk` (calls tikoblkd control API; path = returned `/dev/ublkbN`). Selected
  by `[storage] remote_slow_backing = "s3files_image" | "ublk"` in
  tikovm-host config; `local_fast` path unchanged.
- Suspend/destroy: detach device on destroy/suspend (suspend = snapshot+destroy
  in tikovm); **daemon reserves the dev_id in its registry** so restore
  re-attaches the same `/dev/ublkbN` path that `Snapshot.config` references.
- Also fix while here: set `cache_type: "Writeback"` for remote_slow drives in
  the `PUT /drives` body (`firecracker.rs:750-762` currently uses defaults =
  Unsafe, dropping guest fsync) — required for the FLUSH durability chain.
- e2e: extend `scripts/tikovm/provision.json` manifest with the `archive`
  remote_slow volume (already declared in the echo rootfs) + `source`, add
  volume checks to `run_e2e.sh` (mount LABEL=archive, write/read verify).

## Phases

**Phase 0 — host prep + spike** (validates every risky assumption cheaply)
- Install `linux-modules-extra-6.17.0-1019-aws`, `modprobe ublk_drv`; mount S3
  Files on the host (instance profile, no guest creds).
- PoC with `libublk` (rublk null/loop targets): attach `/dev/ublkbN` to
  Firecracker, verify **snapshot→restore works** with ublk-backed drive, fio
  baseline vs direct-file attach. Go/no-go gate.

**Phase 1 — daemon skeleton**
- `tikoblk` crate, device lifecycle, control API (create/attach/detach/delete),
  registry persistence, systemd unit, mount-ns isolation.
- Recovery: kill -9 daemon → restart → USER_RECOVERY reattach, I/O resumes.
- Tests: unit (registry, control API) + `scripts/tikoblk/run_test.sh` (real
  ublk, loop-file backing, data-verify after recovery).

**Phase 2 — storage engine**
- chunkstore + map/journal + fold, dirty buffer + NVMe write journal + replay,
  read cache LRU, FLUSH durability chain.
- Tests: crash-consistency fuzz (kill daemon/host mid-fio, verify checksums),
  journal replay, cache eviction; fio vs Phase-0 baseline.

**Phase 3 — volume ops**
- COW snapshots/clones (map copy), GC mark-and-sweep, single-attach lease,
  per-volume chunk size, optional zstd (default on).
- Tests: clone shares chunks, GC reclaims only unreferenced, double-attach
  rejected (incl. simulated two-host via remount).

**Phase 4 — tikovm integration**
- RemoteBacking extraction + Ublk impl + config, Writeback cache_type fix,
  provision/destroy/suspend/restore wiring, e2e (echo + archive volume),
  update `docs/tikovm-design.md` §9/§14 + `AGENTS.md`.

**Phase 5 — perf & hardening**
- Buffer sizing per device (memory = queues × depth × io_buf; target
  depth 16–32 × 256–512 KiB so thousands of devices fit; measure), ublk
  zero-copy (`AUTO_BUF_REG`, 6.15+), QUIESCE live-upgrade, per-volume QoS
  (FC rate limiter is bypassed for ublk? no — ublk is a regular drive, FC
  limiter works; add daemon-side limits anyway), metrics, failure drills
  (S3 Files outage → VMs freeze I/O, verify recovery; lost+found drill).

## Risks / open questions

- `libublk` crate API fit (v0.4.6) — Phase 0 spike verifies; fallback is raw
  io_uring + UAPI (protocol is small) or wrapping C libublksrv.
- ublk recovery path has had kernel bugs (CVE-2024-46735) — hammer it in
  Phase 1 tests before building on it.
- S3 Files per-FS ceilings (50k write IOPS, 800 exports/s, 2.7 GB/s) are shared
  per mount — multi-FS sharding/rebalancing is fleet-scale work, out of scope
  here; document the ceiling.
- Hard-mount wedge = all VMs on the host freeze disk I/O; detection/failover
  policy is Phase 5.
- Out of scope: direct-S3-API backend (same chunk engine, later `RemoteBacking`
  impl), dedup/content-addressing, encryption at rest, multi-tenancy authz on
  the control API (host-local UDS only).

## Verification

- `cargo build -p tikoblk && cargo test -p tikoblk` (unit, no ublk needed)
- `cargo clippy -p tikoblk` (allowed — no pgsys dep)
- `scripts/tikoblk/run_test.sh` (ublk integration: lifecycle, recovery, crash
  consistency) and `scripts/tikovm/run_e2e.sh` extended with the archive volume
  — both need the host: `/dev/kvm`, ublk module, S3 Files mount, sudo.
