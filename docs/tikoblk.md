# tikoblk operator runbook

`tikoblkd` serves host block devices (`/dev/ublkbN`) backed by immutable
chunks on an S3 Files mount. Guests (Firecracker VMs) attach them as plain
virtio-block drives. This is the operator guide; architecture details are in
`docs/tikovm-design.md` §9.1.

## Host setup

```bash
sudo scripts/tikoblk/setup_host.sh
```

Idempotent. It: installs `linux-modules-extra`; builds + installs the
ADD_DEV-fixed `ublk_drv.ko` (`scripts/tikoblk/build_ublk_fixed.sh`) into
`/lib/modules/<krel>/updates/`; writes `/etc/modules-load.d/tikoblk.conf`;
installs the udev rule (ublk nodes 0666); installs `tikoblk-module.service`
(boot-time module rebuild) and `tikoblkd.service`; installs `tikoblkd` to
`/usr/local/bin`; enables + starts both units.

Prerequisites: an S3 Files mount at `/mnt/s3files` (fstab entry of type
`s3files` with `_netdev,nofail`), kernel headers for the running kernel, and
`cargo` for the daemon build.

## Module rebuild flow (kernel upgrades)

Ubuntu 6.17.0-10xx's in-tree `ublk_drv` NULL-derefs every ADD_DEV (6.18 NUMA
backport without its call-order prerequisite). The patched module therefore
lives per-kernel in `/lib/modules/<krel>/updates/`. After a kernel upgrade:

- `tikoblk-module.service` (oneshot, before `tikoblkd.service`) runs
  `build_ublk_fixed.sh --boot`: no-ops when the stamped fixed module for
  `$(uname -r)` exists, otherwise rebuilds (source fetched from Launchpad;
  falls back to the offline cache in `/var/lib/tikoblk/module-src/` when the
  network is down) and installs + modprobes it.
- Manual: `sudo bash scripts/tikoblk/build_ublk_fixed.sh` (build+install),
  `--check` (exit 0 = fixed module present, 1 = rebuild needed).
- `tikoblkd` independently smoke-tests ADD_DEV+DEL_DEV at startup and
  refuses to run on a broken driver — if the service fails with
  "ublk control device smoke test failed/timed out", the module is missing
  or broken: rebuild it.

## Control API (HTTP over /run/tikoblk/daemon.sock)

| Route | Description |
|---|---|
| `GET /health` | `{"ok":true}` |
| `GET /metrics` | Prometheus text: per-volume gauges (size, dirty, journal bytes, epoch, attached) + daemon counters (flushes, chunks read/written, cache hits/misses, GC reclaimed, journal replays) |
| `POST /volumes` | `{vol_id, size_mb, backend:"file"|"chunk", chunk_size_kib?, from_snapshot?:"<src>/<snap>"}` → 201 |
| `GET /volumes`, `GET /volumes/{id}` | list / detail (state, generation, epoch, stats, snapshots) |
| `POST /volumes/{id}/attach` | → `{"device":"/dev/ublkbN","formatted":bool}` (mkfs yourself when false) |
| `POST /volumes/{id}/detach` | graceful stop + drain + device delete (409 while mounted) |
| `DELETE /volumes/{id}` | detach if needed, delete volume dir + registry entry (409 while snapshots exist) |
| `POST /volumes/{id}/snapshots` | `{"name"?}` → 201 `{"snap_id"}` (COW, crash-consistent point) |
| `GET /volumes/{id}/snapshots`, `DELETE /volumes/{id}/snapshots/{snap}` | list / delete |
| `POST /gc` | mark-and-sweep pass → `{scanned, reclaimed_count, reclaimed_bytes, ...}` |

Daemon flags: `--ctrl`, `--data-dir` (`/var/lib/tikoblk`), `--sock`,
`--store-root` (`/mnt/s3files/tikoblk`), `--cache-mb` (512),
`--gc-interval-secs` (3600, 0 disables), `--gc-grace-secs` (600).

## Failure modes

- **Daemon death (SIGTERM/SIGKILL/crash)**: devices quiesce
  (UBLK_F_USER_RECOVERY); in-flight I/O stalls, never double-applied. The
  next daemon start's recovery sweep reattaches with the same device node
  and replays leftover NVMe journal segments. SIGTERM is always safe;
  SIGKILL is safe only when I/O is fully quiesced (kill -9 mid-I/O can
  wedge the daemon in D-state until reboot).
- **Store (S3 Files) outage**: chunk reads/writes stall; guest I/O freezes
  (the NFS mount is `hard`). FLUSH only reaches the NVMe journal, so no
  acknowledged data is lost; I/O resumes when the mount recovers. There is
  currently no detection/failover policy.
- **ENOSPC on the data dir**: chunk volumes pre-check free space at create
  (cache + journal headroom); the file backend preallocates. A guest write
  can never hit host ENOSPC mid-I/O on the chunk backend; journal growth is
  bounded by the 64 MiB dirty cap per volume.
- **Device busy on detach**: detach returns 409 while the device has sysfs
  holders (mounts). Umount first.
- **Broken driver**: startup smoke test refuses to start; rebuild the
  module (above).

## Golden rules for the store

- **Never touch volume objects via the S3 API or another host.** All
  store mutations go through the owning daemon (single-daemon-per-store
  assumption; the per-volume flock lease covers attach, not store-wide
  operations). Manual edits under `volumes/` or `chunks/` will corrupt
  maps or leak chunks.
- Deleting a volume never deletes shared pool chunks (clones may reference
  them); only the GC reclaims unreferenced chunks older than
  `--gc-grace-secs`.
- `lost+found`-style salvage: if a store dir is damaged, detach everything
  first; the map + map.journal + journal replay are the recovery order —
  do not improvise.

## GC tuning

`--gc-interval-secs` (periodic pass cadence; 0 = manual `POST /gc` only),
`--gc-grace-secs` (minimum age before an unreferenced chunk is deleted;
protects in-flight flusher writes). GC assumes one daemon per store root.
