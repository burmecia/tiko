# tikovm — General-Purpose VM Management Platform

> Status: **Design** (not yet implemented). New crates, no changes to existing
> `tikod` / `tikoguest` / `core` / `smgr` / `worker` / `pgsys`.

## 1. Goal

Turn the current Tiko-specific microVM orchestration (`tikod` + `tikoguest`) into
a **general-purpose, workload-agnostic VM management platform** that can host
arbitrary serverless workloads — not just Postgres:

- Long-running or scale-to-zero Postgres (the existing Tiko use case)
- Language runtimes (Node.js, Python, Rust, …) — the foundation for a future
  serverless Worker / Lambda-style product
- Scheduled jobs (externalized `pg_cron`, ordinary `cron`)

The platform owns the VM lifecycle, storage, and networking. The workload itself
is **baked into a rootfs image**; neither the host nor the guest daemon knows
anything about Postgres, Node, or cron. The design is fully decoupled from the
Tiko storage stack so the three new crates can be lifted into a standalone repo
with no other workspace dependencies.

## 2. Design principles

1. **Self-describing rootfs.** A workload is a rootfs image + a
   `WorkloadManifest`. Different workloads are just different rootfs images.
   Zero host/guest code changes per workload.
2. **Host = generic infra executor.** Owns VM lifecycle, networking, storage,
   routing, and traffic forwarding. It is deliberately "dumb" about workload
   runtime behavior — it reacts to guest signals and to incoming connections.
3. **Guest = generic in-VM supervisor.** Reads the manifest, runs and supervises
   the process, owns idle detection and health, coordinates suspend/restore.
4. **Clean seam preserved.** The new crates have **no Cargo dependency** on
   `core` / `smgr` / `worker` / `pgsys` (just as today's `tikod`/`tikoguest`
   don't). The only contract between host and guest is a vsock protocol defined
   in the shared `tikovm-protocol` crate.
5. **Observable by default.** Prometheus metrics + structured tracing from day
   one (fills a gap in the current system, which is logging-only).

## 3. Architecture overview

```
                    External clients
                          │
                          ▼
        ┌─────────────────────────────────────────┐
        │  tikovm-hostd  (host daemon)             │
        │                                          │
        │  ┌──────────┐  ┌──────────┐  ┌────────┐ │
        │  │ control  │  │  proxy/  │  │ metrics│ │
        │  │   API    │  │  router  │  │  (prom)│ │
        │  └────┬─────┘  └────┬─────┘  └────────┘ │
        │       │             │                   │
        │  ┌────▼─────────────▼───────────────┐   │
        │  │  node (lifecycle) + control reg.  │   │
        │  │  network(TAP/IPAM) + storage      │   │
        │  │  guestlink (vsock RPC)            │   │
        │  └───────────────┬───────────────────┘   │
        │                  │ vsock + virtio devices │
        └──────────────────┼──────────────────────┘
                           │  (Firecracker microVM)
        ┌──────────────────┼──────────────────────┐
        │  tikovm-guestd (in-VM)  │               │
        │                         ▼               │
        │  ┌──────────────────────────────────┐   │
        │  │ manifest → supervisor            │   │
        │  │ health / idle-evaluator          │   │
        │  │ hostlink (vsock RPC) + server    │   │
        │  │ fs (mount volumes)               │   │
        │  └──────────────┬───────────────────┘   │
        │                 │ supervises             │
        │                 ▼                        │
        │        ┌─────────────────┐               │
        │        │  the workload   │  (rootfs-baked│
        │        │  (echo / PG /   │   binaries)   │
        │        │   node / cron)  │               │
        │        └─────────────────┘               │
        └─────────────────────────────────────────┘
```

## 4. Crate structure

Three new workspace members. None depends on `core`/`smgr`/`worker`/`pgsys`.

```
tikovm-protocol/   shared contract (serde types + vsock framing + errors)
tikovm-host/       host library (lib.rs) + host daemon (bin: tikovm-hostd)
tikovm-guest/      guest library (lib.rs) + guest daemon (bin: tikovm-guestd)
```

### 4.1 `tikovm-protocol` (the contract)

Lightweight — only `serde`, `thiserror`, `libc`. Fixes the current defect where
`tikod` and `tikoguest` duplicate their shared serde types with no shared crate.

```
src/
  lib.rs
  manifest.rs   WorkloadManifest + ProcessSpec, HealthProbe, IdlePolicy,
                RestartPolicy, SuspendHooks, VolumeDecl  (the rootfs schema)
  vm.rs         VmId, VmState (13-variant), VmSpec (provision request),
                ResourceConfig, VmInfo
  volume.rs     VolumeTier { LocalFast, RemoteSlow }
  rpc.rs        HostToGuest, GuestToHost message enums
  routing.rs    RoutingRule { Http, Tcp, Token }
  error.rs      ProtocolError + {"error":{kind,message}} envelope helpers
  codec.rs      length-delimited JSON framing over a vsock byte stream
```

### 4.2 `tikovm-host` (lib + `tikovm-hostd`)

```
src/
  lib.rs
  main.rs          tikovm-hostd: clap, load config.toml, spawn API + proxies + vsock
  config.rs        HostConfig (TOML): listen addrs, asset dirs, network pool,
                   vsock base CID, defaults
  vmm/
    mod.rs         Vmm trait + coarse BackendState (Created/Started/Paused/Destroyed)
    firecracker.rs FirecrackerBackend (ported from tikod; +vsock device;
                   +virtio-block volumes; drops tiko.env seeding → generic
                   env + manifest injection)
    stub.rs        StubBackend (non-Linux; compiles on macOS for API/proxy dev)
  node.rs          lifecycle orchestration: create/start/pause/resume/suspend/
                   restore/destroy; enforces the transition table
  control.rs       registry: DashMap<VmId, VmRecord> + single-flight restore
                   locks + cancel Notifys
  network/
    mod.rs         Ipam: alloc/release from a configurable subnet pool
    tap.rs         TAP create/destroy + NAT/iptables (ported)
    vsock.rs       per-VM CID allocation + UDS path mgmt + vsock_override at restore
  storage/
    overlay.rs     per-VM rootfs overlay create/seed (ported; generalized injection)
    volume.rs      VolumeProvisioner: LocalFast + RemoteSlow (RemoteBacking trait)
    snapshot.rs    snapshot dir mgmt (ported)
  proxy/
    router.rs      routing table + wake-on-connect + warm-pause keepalive toggle
    http.rs        HTTP reverse proxy (route by Host / path / X-Tiko-Endpoint)
    tcp.rs         generic TCP passthrough (route by listener port or first-byte token)
    guest_proxy.rs generic guest proxy (tunnel arbitrary HTTP to the guest)
  guestlink.rs     GuestLink: vsock RPC client (Start/Stop/PreSuspend/GetHealth)
                   + serves GetNetworkStats
  api/
    server.rs      GENERIC control API (see §10)
    client.rs      ApiClient for tests
  metrics.rs       prometheus endpoint
```

### 4.3 `tikovm-guest` (lib + `tikovm-guestd`)

```
src/
  lib.rs
  main.rs          tikovm-guestd: load manifest, start supervisor + hostlink + vsock server
  manifest.rs      load /etc/tikovm/workload.toml (+ injected override)
  supervisor.rs    spawn ProcessSpec, restart w/ backoff, signal forwarding,
                   graceful stop, liveness (big upgrade over current tikoguest)
  health.rs        run HealthProbe (http/tcp/exec) on interval; report to host
  idle.rs          the generalized idle evaluator (see §8)
  hostlink.rs      vsock RPC client: Ready, HealthReport, SuspendRequest, ShutdownRequest
  server.rs        vsock RPC server: Start/Stop/PreSuspend/PostRestore from host;
                   HTTP-tunnel forwarder for the guest proxy
  lifecycle.rs     suspend/restore coord: run pre_suspend_cmd, wait quiesce, ack;
                   post_restore_cmd on restore
  fs.rs            mount declared volumes at boot; report volume readiness
  metrics.rs       generic process metrics (cpu/mem/proc activity)
```

## 5. Core model: the self-describing rootfs

A workload is shipped as a rootfs image containing:

- the workload's binaries / runtime,
- `/etc/tikovm/workload.toml` — the `WorkloadManifest`,
- `/usr/local/bin/tikovm-guestd` — the generic guest daemon,
- any workload-specific helper scripts the manifest references (idle probes,
  control binaries, etc.).

The generic guest daemon reads the manifest and runs whatever the rootfs
contains. A different workload (Node, Python, Postgres) is **just a different
rootfs** — no host or guest daemon changes.

### 5.1 `WorkloadManifest` (guest-only schema)

```toml
# /etc/tikovm/workload.toml
version = 1
workload = "echo"                      # informational label

[process]                              # the supervised main process
cmd = "/usr/local/bin/echo-server"
args = ["--port", "8080"]

[init]                                 # optional one-time bootstrap, runs before [process]
cmd = "/usr/local/bin/bootstrap.sh"

[health]                               # how the guest health-checks the workload
kind = "http"                          # http | tcp | exec | none
path = "/health"
port = 8080
interval_secs = 5

[idle]                                 # scale-to-zero; the GUEST owns this
tick_secs = 5
idle_secs = 120
[[idle.probes]]
kind = "host_network"                  # pull VM-scoped network stats from host via vsock
[[idle.probes]]
kind = "exec"                          # workload-specific metrics (script baked in rootfs)
cmd = "/usr/local/bin/idle_check.sh"

[suspend]                              # quiesce hooks for a clean snapshot
pre_suspend_cmd = "/usr/local/bin/quiesce.sh"
post_restore_cmd = "/usr/local/bin/resume.sh"

[restart]                              # restart policy for the supervised process
policy = "on_failure"                  # always | on_failure | never
backoff_secs = 2

[expose]                               # workload HTTP exposed externally via guest proxy
http_port = 8080                       # guest proxy forwards external HTTP here
control_bin = "/usr/local/bin/workload-control"  # optional /db,/pitr-style control routes

[[volumes]]                            # declared storage needs
name = "data"
tier = "local_fast"                    # local_fast | remote_slow
mount_path = "/mnt/data"
size_mb = 1024                         # local_fast only
```

Everything in the manifest is **guest-internal behavior**. The host reads **only
the `[[volumes]]` section** (at provision time, to create/attach storage before
boot). It never reads `idle`, `health`, `process`, `suspend`, or `restart`.

### 5.2 `VmSpec` (the provision request — host/infra)

```rust
struct VmSpec {
    vm_id: VmId,
    rootfs: RootfsRef,                    // base image path/ref
    resources: ResourceConfig,            // memory_mb, vcpus
    kernel: KernelSpec,                  // path, cmdline, initrd
    network: NetworkSpec,                // derived or explicit
    routing: Vec<RoutingRule>,           // how external traffic reaches this VM
    env: HashMap<String, String>,        // injected environment
    manifest: Option<WorkloadManifest>,  // authoritative; host reads only .volumes
}
```

Routing, ports, resources, env, and the manifest are host-specified at provision
time. Lifecycle/idle policy is **not** here — it lives in the manifest and is
driven by the guest.

## 6. VM state machine

**Stable states:** `Created`, `Started`, `Paused`, `Suspended`, `Destroyed`
**Transitional states:** `Creating`, `Starting`, `Pausing`, `Resuming`,
`Suspending`, `Restoring`, `Destroying`

```
                ┌─────────┐
                │Creating │
                └────┬────┘
                     ▼
                ┌─────────┐  start   ┌─Starting─┐    ┌─────────┐
                │ Created │─────────▶│          │───▶│ Started │◀─┐
                └─────────┘          └──────────┘    └────┬────┘  │
                                                       pause │     │ resume
                                                         ▼   │     │
                                                      ┌─Pausing─┐  │
                                                      └────┬────┘  │
                                                           ▼       │
                ┌─────────┐  suspend  ┌─Suspending─┐   ┌────────┐  │
                │Suspended│◀──────────│            │◀──│ Paused │──┘
                └────┬────┘           └────────────┘   └────────┘
                     │ restore
                     ▼
                  ┌─Restoring─┐──────────────────────────▶ Started

   any stable ──▶ Destroying ──▶ Destroyed   (terminal)
```

| Operation | From → transitional → To | Composed Vmm ops |
|---|---|---|
| `create` | – → Creating → Created | `create_vm` |
| `start` | Created → Starting → Started | `start_vm` |
| `pause` | Started → Pausing → Paused | `pause_vm` |
| `resume` | Paused → Resuming → Started | `resume_vm` |
| `suspend` | Paused → Suspending → Suspended | `snapshot_vm` → `destroy_vm` |
| `restore` | Suspended → Restoring → Started | `restore_vm` (+`vsock_override`) → `resume_vm` |
| `destroy` | any stable → Destroying → Destroyed | `destroy_vm` |

**Semantics:**
- **pause** = warm freeze (VM stays loaded in memory; fast resume).
- **suspend** = cold freeze: snapshot memory+state to disk **and** tear down the
  process, freeing host RAM. The on-disk snapshot is retained so the VM can be
  restored by `vm_id` alone. Only valid from `Paused`.
- **Scale-to-zero of a running VM** = `pause` (Started→Paused) then `suspend`
  (Paused→Suspended), driven by the guest's `SuspendRequest`.
- **restore** = wake path: reload from snapshot + resume (Suspended→Started).
  Proxy wake-on-connect invokes this.

The fine-grained `VmState` lives in `tikovm-protocol` and is tracked by the
**control registry** (`VmRecord.state`). The `Vmm` backend trait reports only
coarse live states (`Created`/`Started`/`Paused`/`Destroyed`); transitional +
`Suspended` (snapshot retained, no live VM) are control-layer concepts. The
`Vmm` trait barely changes from today (state rename only); `Node` + the registry
express the full machine and reject illegal transitions with `VmmError::InvalidState`.

This is exactly what `tikod/src/node/mod.rs` already composes (`cold_freeze` =
snapshot+destroy on a paused VM; `thaw` = restore+resume; `warm_pause` = pause),
now expressed as an explicit state machine with clearer naming.

## 7. vsock control protocol

Host ↔ guest runs over **virtio-vsock** (replacing today's HTTP-over-TAP on
:9000). Firecracker supports it (`PUT /vsock` with `guest_cid` + `uds_path`;
host connects via AF_UNIX, sends `CONNECT <port>\n`). The CI microvm kernel 6.1
already enables `CONFIG_VIRTIO_VSOCKETS`.

**Framing:** length-delimited JSON (`tikovm-protocol/codec.rs`).

**Messages:**
- `HostToGuest`: `Start`, `Stop`, `PreSuspend`, `PostRestore`, `GetHealth`,
  `GetNetworkStats` (host answers its own — VM-scoped traffic stats), `Exec{cmd}`
- `GuestToHost`: `Ready`, `HealthReport`, `SuspendRequest` ("I'm idle, suspend
  me"), `ShutdownRequest` ("I'm done, destroy me")

**Snapshot/restore caveats (validated against Firecracker docs):**
1. On `snapshot/create` the vsock device is **reset**; in-flight vsock
   connections drop but **listen sockets survive** and accept new connections
   after resume. Fine for our reconnect-per-RPC model.
2. The host UDS path collides on restore, so `restore` **must pass
   `vsock_override`** in the `/snapshot/load` call to give the restored VM a
   fresh host socket path.

**Why vsock over the old TCP-on-TAP:** available before the guest IP/network is
up; survives TAP teardown; no port collision with data traffic; cleaner
scale-to-zero coordination. The current code sets `"vsock": null`
(`vm_config.json:46`) — the new backend adds the device and uses
`vsock_override` at restore.

## 8. Idle detection (guest-authoritative)

The guest **owns** the idle signal. The host never knows whether a workload is
"idle" — it only provides a network-stat source and obeys the guest's verdict.
Two signal types, two locations:

| Signal | Where it's knowable | Why |
|---|---|---|
| Workload-internal (PG active backends, in-flight jobs, …) | In-VM only | Host sees bytes, not semantics |
| Network (connections, last-data-byte) | Both, but host is authoritative | Proxy is the only component that sees every connection; last-data-byte defeats keepalive false-positives |

**Flow:**
1. Each `tick_secs`, the guest collects every declared probe:
   - `host_network` → vsock `GetNetworkStats` → `{conns, last_data_age_secs, bytes}` (VM-scoped, all ports — needs no port config in the manifest).
   - `exec` / `http` → workload-internal metrics (the PG specifics from `tikoguest/src/pgmetrics.rs:123-132` move into a rootfs script).
2. Guest evaluates the `[idle]` policy: idle = every probe idle, sustained for `idle_secs`.
3. Guest → host `SuspendRequest`. Host obeys: `pause` (warm) → countdown → `suspend` (cold); cancels if a real connection arrives during the countdown (ported from `tikod/src/api/server.rs:493-589`).
4. Wake-on-connect stays host-side (the guest is frozen while paused — it can't participate).

This preserves Tiko's proven scaler + proxy-wake + pause-epoch machinery
(`tikoguest/src/scaler.rs:73-187`, `tikod/src/proxy/mod.rs:280-386`,
`control/mod.rs:61`), just with the PG-specific SQL generalized into a rootfs
probe. A `ShutdownRequest` covers ephemeral/scheduled workloads (run to
completion, then signal the host to destroy).

## 9. Storage: 2-tier volumes

Two optional storage tiers, **declared in the rootfs manifest** (`[[volumes]]`):

| Tier | Backing | Lifetime | Sizing |
|---|---|---|---|
| `local_fast` | per-VM ext4 image on host-local disk, attached as virtio-block | survives suspend; **ephemeral on destroy** | capped (`size_mb`) |
| `remote_slow` | host-mounted remote FS, attached as virtio-block (image on the mount) | **persists across destroy** | unlimited (backend-enforced) |

**`local_fast`** generalizes today's per-VM overlay (`/dev/vdb`) +
`DriveConfig`. The host creates a sparse ext4 image of `size_mb`, attaches it as
a virtio-block device; the guest mounts it at `mount_path`.

**`remote_slow`** exposes slow, durable, shared-capable storage to the guest.
Firecracker **does not implement virtio-fs** (its device set is only
virtio-block, vhost-user-block, virtio-net, virtio-vsock, virtio-rng,
virtio-pmem, virtio-mem — confirmed in Firecracker's `device-api.md`). The
leading implementation is therefore:

- **virtio-block from host-mounted remote** (recommended): the host mounts S3
  Files (or any remote FS) and attaches an image file on that mount as a
  virtio-block device. The guest sees a plain block device and mounts it — **no
  NFS client, no credentials, no backend awareness in the guest**. Keeps the
  host-owned / guest-generic property we wanted from virtio-fs. Survives destroy
  (the backing file persists on the remote mount).
- **NFS-in-guest** (fallback): the guest itself mounts the remote FS (the proven
  current Tiko approach, `mount_s3files_vm.sh`). Simpler, but re-couples the
  guest/rootfs to the backend and puts credentials in the guest.
- **vhost-user-block** (future production scale path): an external daemon serves
  remote-backed block storage; noted, not built initially.

Both tiers live behind a `RemoteBacking` trait in `tikovm-host/storage`; the
protocol-level `VolumeSpec` stays identical regardless of backing. Both tiers are
optional — the echo demo declares none.

**Validation against real Tiko/PG:** the PG workload would declare a `local_fast`
`cache` volume (→ `TIKO_LOCAL_PATH`) and a `remote_slow` `archive` volume
(→ `TIKO_STORAGE_ROOT`). The 2 tiers express precisely the local-cache-vs-
durable-archive split the storage engine already assumes — now without the
generic layer knowing anything about PG chunks.

**Provisioning mechanism (confirmed):** Firecracker attaches block devices at
VM-create time, before the guest boots, so the host must learn the volume
declarations at provision. The provision request carries the manifest
(operator-provided, authoritative); the host reads **only its `[[volumes]]`
section** to create/attach storage before boot, then injects the manifest into
the guest. The host never reads `idle`/`health`/`process`.

## 10. Control API & generic guest proxy

The host API (`api/server.rs`) is **generic only** — it drops the old
`/db`, `/pitr`, `/branch` route families. Workload-specific endpoints are
exposed via a generic guest proxy that tunnels arbitrary HTTP to the guest, so a
rootfs workload can surface whatever it wants (`/db`, `/pitr`, app APIs) without
the host knowing the routes.

**Control API (operator-facing, host `:9000`):**
```
GET  /health
GET  /vms                         # inventory (merged live + registered)
PUT  /vms                         # create (auto vm-{N} id)
POST /vms/provision               # create + start + register
GET  /vms/{id}                    # state
DELETE /vms/{id}                  # destroy
GET  /vms/{id}/ip
POST /vms/{id}/{start,pause,resume,suspend,restore,destroy}
POST /vms/{id}/reports            # agent pushes metrics; returns pause_epoch
POST /vms/{id}/pause-request      # (legacy alias of suspend-request)
POST /vms/{id}/suspend-request    # guest: "suspend me"
POST /vms/{id}/shutdown-request   # guest: "I'm done, destroy me"
ANY  /vms/{id}/guest/{path}       # control passthrough → guest agent
```

**Generic guest proxy (two surfaces, both vsock-tunneled):**
- **Control passthrough** — `ANY /vms/{id}/guest/{path}` → guest agent → its
  registered handlers (a rootfs-extensible service hook generalizing
  `tikoguest/src/service.rs`). This is where workload control like `/db`/`/pitr`
  lives, now served **from the rootfs** (a `control_bin` or registered service),
  not hardcoded in the daemon.
- **Data proxy** (host proxy port, public-facing) — matched by `RoutingRule` →
  guest agent → forwarded to the workload's declared `expose.http_port`. This is
  the external app-traffic path.

The host pipes HTTP generically; all route meaning stays in the rootfs. This
replaces the hardcoded route families in both `tikod/src/api/server.rs` and
`tikoguest/src/server.rs`.

## 11. Routing / proxy (generalized)

Replaces the old PG-wire-proxy + PostgREST-proxy with one config-driven router:

- `RoutingRule::Http { host | path | header }` → HTTP reverse proxy to the
  workload's http port (via the guest proxy tunnel).
- `RoutingRule::Tcp { listener_port }` → dedicated host port passthrough (for
  non-HTTP / wire protocols).
- `RoutingRule::Token` → first-bytes selector (generalizes the `tiko.endpoint`
  trick for wire protocols).

Wake-on-connect + warm-pause keepalive logic is ported from
`tikod/src/proxy/mod.rs:280-386` and made port-agnostic. For the echo demo,
HTTP-header routing (`X-Tiko-Endpoint: vm-N`) proves the path.

## 12. Observability

- **Metrics:** prometheus scrape endpoint on `tikovm-hostd`
  (`vm_count_by_state`, freeze/wake latency histograms, proxy connections,
  per-VM CPU/mem reported by the guest over vsock). Fills the current gap —
  today's system is logging-only.
- **Tracing:** structured `tracing` spans keyed by `vm_id` for
  provision/pause/suspend/restore/proxy.
- **Health:** liveness vs readiness endpoints; per-VM serial logs retained
  (ported from `tikod/src/vmm/firecracker.rs`).
- **Config:** a real `config.toml` (replaces today's CLI-args-only model).

## 13. Demo workload (validates end-to-end)

A trivial **HTTP echo server** rootfs:
- `workload.toml`: `[process]` = echo-server :8080; `[health]` = `GET /health`;
  `[idle]` = scale_to_zero 120s + `host_network` probe; `[expose]` http_port 8080.
- A tiny echo binary (small Rust static binary, or busybox httpd) baked into a
  minimal rootfs (no PG, no PostgREST, no S3 mount).
- Build script `scripts/tikovm/build_echo_rootfs.sh` (modeled on
  `create_rootfs.sh` but minimal).

**Validates:** provision → guest reads manifest → runs+supervises echo → HTTP
routing wired → `curl` reaches echo → idle → `SuspendRequest` → suspend →
`curl` wakes → restore → `ShutdownRequest`/destroy. Proves the whole generic
loop with zero workload-specific code in host/guest.

## 14. What's deferred (designed, not built initially)

- Migrating the real Tiko Postgres to a rootfs + manifest (validates against the
  real workload; first real user of both storage tiers).
- Language-runtime (Lambda-style) and cron workloads.
- `vhost-user-block` production scale path for `remote_slow`.
- Multi-node clustering / distributed registry.
- Garbage collection of snapshots / overlays.

## 15. Relationship to the existing Tiko code

**Reused (ported as fresh, generalized code):**
- `Vmm` trait + `FirecrackerVmm` lifecycle → `tikovm-host/vmm/` (+ vsock device, + virtio-block volumes).
- `Node` orchestration → `tikovm-host/node.rs` (renamed ops, transition enforcement).
- Deterministic per-VM networking, TAP/NAT → `tikovm-host/network/`.
- Overlay create/seed, snapshot mgmt → `tikovm-host/storage/`.
- Proxy wake-on-connect + warm-pause keepalive → `tikovm-host/proxy/`.
- Scaler idle pattern + pause-epoch coordination → `tikovm-guest/idle.rs` (generalized).
- `Service` trait extensibility scaffold (`tikoguest/src/service.rs`) → the guest's rootfs-extensible control handlers.

**Dropped / replaced:**
- All PG-specific routes (`/db`, `/pitr`, `/branch`) on both sides → generic guest proxy.
- PG wire-protocol proxy + PostgREST HTTP proxy → config-driven router.
- `pgops.rs`, `pgmetrics.rs`, `postgrest.rs`, `backup.rs` (all PG-specific) → manifest-declared probes/hooks in the rootfs.
- `tiko.env` hardcoded identity seeding → generic env + manifest injection.
- `VmRecord.pg_port` / `branch_id` / `tenant_id` → generic `spec` + `state` + `metrics`.
- Duplicated serde types across tikod/tikoguest → `tikovm-protocol` shared crate.
- TCP-over-TAP control channel → vsock.

**Untouched:** `core`, `smgr`, `worker`, `pgsys`, `cli`, and the existing
`tikod` / `tikoguest` crates are not modified.

## 16. Build & verification

- `cargo build` / `cargo test -p tikovm-protocol` / `-p tikovm-host` /
  `-p tikovm-guest` — all must build on macOS (stub backend) and Linux
  (Firecracker).
- Tests: codec/manifest round-trips; `MockVmm` + fake guestlink (mirroring
  `tikod/tests/db_routes.rs`); supervisor restart + health + idle-evaluator.
- `clippy` is safe on these crates (no `pgsys` FFI in the dependency set).
- Echo boot test modeled on `tikod/examples/boot_test.rs` (Linux + KVM only).

## 17. Implementation sequencing

1. Scaffold 3 crates + workspace registration + `tikovm-protocol` (types, codec,
   errors, state machine).
2. `tikovm-host`: `Vmm` trait + stub + port Firecracker (+vsock); `Node`
   (transition enforcement); control registry; config.
3. `tikovm-host`: networking (TAP/IPAM/vsock); storage (overlay + volume
   provisioner).
4. `tikovm-host`: proxy/router + guest proxy + guestlink + API + metrics.
5. `tikovm-guest`: manifest + supervisor + health + idle + hostlink/server +
   lifecycle + fs.
6. Echo rootfs + boot test.
7. Tests across all crates.
