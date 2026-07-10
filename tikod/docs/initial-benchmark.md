# Initial insert/checkpoint benchmark

Baseline throughput measured on **2026-07-02**, comparing Tiko's storage
root on **local disk** vs the **S3 Files mount** (`/mnt/s3files`). Purpose:
establish where time goes when writing a large table through the Tiko
storage manager, before any storage-layer optimization.

---

## Environment

- **Firecracker microVM**: 2 vCPU, 512 MB RAM (`tikod/scripts/vm_config.json`).
  - `shared_buffers` ≈ 128 MB (default 25% of RAM) — note this is smaller than
    the test table, so scans are cache-thrashing, not truly warm.
- **Rootfs**: 5 GB sparse ext4 image (`tikod/assets/ubuntu-24.04-rootfs.ext4`),
  built by `scripts/create_rootfs.sh`.
- **Postgres**: Tiko build, `s3smgr` storage manager registered.
- **Tiko I/O unit**: **chunk = 256 KB** (32 × 8 KB blocks). All throughput
  numbers below can be read as chunks/s by dividing MB/s by 0.256.
- **S3 Files path** (when storage root is on the mount):
  `guest → efs-proxy (userspace TLS) → tap0 → host MASQUERADE → mount target
  172.31.38.90 → S3 bucket`. NFSv4.2. Filesystem nearly empty (~97 MB used).

## Workload

`tikod/scripts/bench_insert.sql`:
- 250,000 rows, `payload` ≈ 1 KB each → **284 MB total relation size**
  (table + primary-key index).
- Phases (each timed via `\timing on`):
  1. `INSERT ... SELECT generate_series(...)` (bulk insert)
  2. `CHECKPOINT` (flush dirty buffers through Tiko to the storage root)
  3. `SELECT count(*)` full table scan

Run as: `psql -d postgres -f tikod/scripts/bench_insert.sql`

Storage root switched between runs by setting `TIKO_STORAGE_ROOT`:
- local: under `/var/lib/postgresql/` (root ext4 disk)
- mount: under `/mnt/s3files/` (S3 Files)

---

## Results

284 MB relation ≈ **~1,136 chunks**.

| Phase | Local disk | S3 Files | Slowdown |
|-------|-----------|----------|----------|
| Bulk INSERT (250k rows) | 7.5 s — 37.7 MB/s, 33k rows/s | 12.1 s — 23.5 MB/s, 20.7k rows/s | 1.6× |
| **CHECKPOINT** (remote flush) | **1.9 s — 146 MB/s** | **12.7 s — 22.3 MB/s** | **6.5×** |
| Full table scan | 6.9 s — 41 MB/s | 11.2 s — 25.4 MB/s | 1.6× |

Chunk-rate view of CHECKPOINT (the storage-write headline):

| Backend | chunks/s | per-chunk latency |
|---------|----------|-------------------|
| Local disk | ~598 | ~1.7 ms |
| S3 Files | ~89 | ~11 ms |

## Analysis

1. **The mount is the bottleneck, not Tiko's per-chunk handling.** On local
   disk the same code path sustains ~598 chunks/s (146 MB/s); the S3 Files
   mount caps it at ~89 chunks/s (22 MB/s). So the storage manager itself is
   not the ceiling — the remote backend is.
2. **CHECKPOINT is hit hardest (6.5×).** It is pure sequential write of ~1,136
   dirty chunks, and each chunk pays near-full per-NFS-op round-trip cost
   through the userspace `efs-proxy` (TLS) + tap/NAT path. Local disk + host
   page cache amortizes these at 146 MB/s; the mount does not.
3. **INSERT and scan are only 1.6× slower** because they are not pure device
   throughput: INSERT is partly CPU-bound (`generate_series` + `md5` + WAL on
   local disk) and only hits the storage write path once `shared_buffers`
   fills (~128 MB); the scan re-reads from storage because the 284 MB table
   does not fit in shared buffers.
4. **~22 MB/s is the mount's symmetric read/write ceiling** for this workload
   (read and write both land at 22–25 MB/s on S3 Files).

## Tuning directions (not yet attempted)

1. **Coalesce/batch chunk writes** in the checkpoint / `store_ops` path —
   write larger runs per relation/fork per syscall instead of one chunk at a
   time. This directly attacks the per-NFS-op latency (~11 ms/chunk) that
   dominates CHECKPOINT. Expected to move CHECKPOINT toward the local number.
2. **More RAM → bigger `shared_buffers`** (bump `mem_size_mib` in
   `vm_config.json`). Reduces checkpoint frequency/pressure and lets the scan
   hit memory instead of re-reading from storage.
3. **Check EFS/S3 Files burst credits and size-based throughput.** A nearly
   empty filesystem may be on a low baseline; the ceiling could lift with
   more stored data or burst headroom. Also consider whether the single
   `efs-proxy` connection is serializing writes.

## Open questions for the next round

- Does larger I/O (multi-chunk coalescing) close the CHECKPOINT gap?
- Is the 22 MB/s the mount's true ceiling, or the `efs-proxy`/NAT path's?
  (Test: `dd`/`fio` directly on `/mnt/s3files` to separate Tiko from the mount.)
- How does throughput scale with the 1M-row variant (table > shared_buffers
  by a larger margin → more eviction during INSERT)?

## Reproduce

```bash
# In the VM, as postgres, storage root on local disk:
TIKO_STORAGE_ROOT=/var/lib/postgresql/tiko_root \
TIKO_LOCAL_PATH=/var/lib/postgresql/tiko_local \
  psql -d postgres -f tikod/scripts/bench_insert.sql

# Same test, storage root on the S3 Files mount:
TIKO_STORAGE_ROOT=/mnt/s3files/tiko_root \
TIKO_LOCAL_PATH=/var/lib/postgresql/tiko_local \
  psql -d postgres -f tikod/scripts/bench_insert.sql
```

To isolate the mount from Tiko (sanity-check the 22 MB/s ceiling):

```bash
# inside the VM, as root:
dd if=/dev/zero of=/mnt/s3files/_dd bs=256k count=4096 oflag=direct conv=fdatasync
```
