# tikovm — General-Purpose VM Management Platform

> Status: **Design + implementation in progress.** Three new crates
> (`tikovm-protocol`, `tikovm-host`/`tikovm-hostd`, `tikovm-guest`/`tikovm-guestd`)
> with **no changes** to the existing `tikod` / `tikoguest` / `core` / `smgr` /
> `worker` / `pgsys` and no Cargo dependency on them.
>
> **Implemented and validated on KVM** (Ubuntu 24.04 + Firecracker v1.17-dev):
> the 13-state lifecycle (incl. suspend/restore), SQLite crash recovery, the
> scheduler, the generic guest supervisor, and the **scale-to-zero loop** over
> the vsock control channel (idle → suspend → wake-on-connect). See the
> [Implementation status](#-implementation-status) appendix for the full matrix
> of what is built vs. designed.

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
  scheduler.rs     long-running tokio task: evaluates due cron schedules and
                   triggers Node restore/start (see §13)
  control.rs       registry: DashMap<VmId, VmRecord> + single-flight restore
                   locks + cancel Notifys
  store.rs         StateStore trait + SQLite impl: durable source of truth for
                   the registry; write-through + crash recovery (see §14)
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

[schedule]                             # scheduled jobs (host-driven wakeups; optional)
cron = "*/5 * * * *"                   # or interval_secs = 300
keep_warm = true                       # suspend between runs (default); false = ephemeral

[expose]                               # workload HTTP exposed externally via guest proxy
http_port = 8080                       # guest proxy forwards external HTTP here
control_bin = "/usr/local/bin/workload-control"  # optional /db,/pitr-style control routes

[[volumes]]                            # declared storage needs
name = "data"
tier = "local_fast"                    # local_fast | remote_slow
mount_path = "/mnt/data"
size_mb = 1024                         # local_fast only
```

Everything in the manifest is **guest-internal behavior** except the two sections
the host must execute. The host reads **only `[[volumes]]` and `[schedule]`** (at
provision time, to create/attach storage before boot and to drive scheduled
wakeups). It never reads `idle`, `health`, `process`, `suspend`, or `restart`.

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
    manifest: Option<WorkloadManifest>,  // authoritative; host reads .volumes + .schedule
    schedule: Option<SchedulePolicy>,    // overrides manifest [schedule] if set
}
```

Routing, ports, resources, env, and the manifest are host-specified at provision
time. The schedule may be overridden here (operators tune cadence without
rebuilding the rootfs). Idle policy is the one lifecycle concern that is
**manifest-only and guest-driven** — never host-specified.

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

> **Status: implemented & validated on KVM.** `tikovm-host/guestlink.rs`
> (per-VM AF_UNIX server on `{vsock_uds}_9001`) + `tikovm-guest/hostlink.rs`
> (`VsockHostLink`, AF_VSOCK client to CID 2:9001). The guest→host direction
> (`GetNetworkStats`, `Suspend`) drives the scale-to-zero loop; the host→guest
> command direction (`Start`/`Stop`/`PreSuspend`/`PostRestore`) is defined but
> not yet wired (the loop currently needs only guest→host + wake-on-connect).

Host ↔ guest runs over **virtio-vsock** (replacing today's HTTP-over-TAP on
:9000). Firecracker supports it (`PUT /vsock` with `guest_cid` + `uds_path`).
The CI microvm kernel 6.1 already enables `CONFIG_VIRTIO_VSOCKETS`.

Two directions:
- **Guest → host (control):** the guest connects (AF_VSOCK) to the host
  (CID 2) on port 9001. Firecracker forwards this to the host's per-VM AF_UNIX
  socket at `{vsock_uds}_9001`, so the host derives the target VM from *which
  socket* the connection arrived on — the messages carry no `vm_id`.
- **Host → guest (commands):** the host connects to the guest's AF_VSOCK
  listener (port 9000) by connecting to the UDS and sending
  `CONNECT <port>\n`.

**Framing:** length-delimited JSON (`tikovm-protocol/codec.rs`).

**Messages:**
- `GuestToHost` (guest→host): `GetNetworkStats`, `Suspend`, `Shutdown`,
  `Ready{workload}`, `HealthReport{healthy}`.
- `HostReply` (host→guest): `Stats(NetworkStats)`, `Suspended{pause_epoch}`,
  `Ok`, `Error{message}`.
- `HostToGuest` (host→guest, defined): `Start`, `Stop{mode}`, `PreSuspend`,
  `PostRestore`, `GetHealth`.

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

## 13. Scheduling (host-driven triggers)

Sibling to idle detection (§8), but **host-driven**: only the host can wake a
suspended VM on time, so scheduling must be host-owned. Idle is guest-driven
(the guest knows its own activity); scheduling is clock-driven (the host owns
the clock). Both end in the same `suspend`/`restore` machinery and guest signals.

**Where the crontab lives.** Declared in the manifest `[schedule]` block; the
host reads it at provision (same rule as `volumes`) and stores it in `VmRecord`.
The provision request may **override** it (`schedule` field, §5.2), so operators
can tune cadence without rebuilding the rootfs.

**Module:** `tikovm-host/scheduler.rs` — a long-running tokio task, wakes on a
timer, evaluates due schedules (standard cron expressions, or `interval_secs`),
and invokes `Node`.

**Run modes** (`keep_warm`):
- `keep_warm = true` (default): provision once → `suspend`; each tick the
  scheduler `restore`s the VM (wake) → the guest supervisor runs `[process]`
  fresh (on-wake / `post_restore` semantics) → on completion the guest sends
  `SuspendRequest` → host `suspend`s until the next tick.
- `keep_warm = false` (ephemeral): each tick the scheduler provisions a fresh VM
  → `[process]` runs → `ShutdownRequest` → `destroy`. Lambda-like; pays boot
  cost per run; simplest state model.

**The trigger *is* the wake** — no separate `Run` RPC. Restoring/starting the VM
is the trigger; the guest's supervisor treats a wake as a job invocation and
runs `[process]`. On completion the guest uses the same signals as idle
(`SuspendRequest` / `ShutdownRequest`). So the only difference from idle-driven
scale-to-zero is *who pulls the trigger*: the host's clock vs. the guest's idle
evaluator.

`next_fire_time` is persisted (§14) so the scheduler resumes correctly after a
hostd crash.

## 14. Persistence & crash recovery

The current `tikod` control registry is in-memory only (`DashMap`), so a hostd
crash loses all VM records. `tikovm-host` fixes this with a durable store and a
reconcile-on-boot recovery model.

**Module:** `tikovm-host/store.rs` — a generic `StateStore` trait
(`upsert_vm`, `get_vm`, `list_vms`, `delete_vm`) with a **SQLite** impl now
(`rusqlite`/`sqlx`), swappable for Postgres/etcd later (multi-node). DB file at
`data_dir/tikovm.db`, path in `HostConfig`.

**Model:** the DB is the durable source of truth; the in-memory `DashMap`
remains the hot read path and is reconstructed from the DB on boot.

**Write-through:** every registry mutation persists — `create`, each state
transition, `suspend`→snapshot descriptor, schedule, `pause_epoch`.
`metrics`/`last_activity` persist at low frequency / on state change (not
per-connection; a slightly-stale `last_activity` on recovery is acceptable).

**Boot reconciliation (controller pattern):** read DB → rebuild registry →
probe each VM → correct drift → resume scheduler / idle timers / proxy.

| VmRecord state at crash | Reconciliation on restart |
|---|---|
| `Suspended` | verify snapshot files exist on disk → keep `Suspended` (restorable). **Trivial — the common case** in a scale-to-zero system. |
| `Started` / `Paused` | Firecracker child is likely dead (hostd held the `Child` with `kill_on_drop`). **Restore-on-demand:** mark `Suspended` from the last snapshot; the proxy/scheduler lazily `restore`s on next access. |
| `Created` / `Destroyed` | no live runtime → restore as-is. |

Plus: re-create missing TAPs / iptables, re-establish vsock UDS paths, recompute
scheduler `next_fire_time` from cron. The **guest is transparent** to hostd
crashes — it keeps running its supervised process and only sees a vsock reset
(which it already handles per §7).

**Policy (confirmed): restore-on-demand.** hostd does not try to keep
Firecracker children alive across its own crash; a crashed running VM is
restored lazily from its last snapshot on next access. Cost: loss of in-memory
state since the last snapshot. This is **core scope** (not deferred) and is
validated by a crash/restart test.

## 15. Demo workload (validates end-to-end)

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

## 16. What's deferred (designed, not built initially)

- Migrating the real Tiko Postgres to a rootfs + manifest (validates against the
  real workload; first real user of both storage tiers).
- Concrete language-runtime (Lambda-style) and cron-job **rootfs workloads** (the
  scheduling mechanism itself is in-scope — see §13).
- `vhost-user-block` production scale path for `remote_slow`.
- Multi-node clustering / distributed registry (the `StateStore` trait is the
  seam; §14).
- Garbage collection of snapshots / overlays.

## 17. Relationship to the existing Tiko code

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

## 18. Build & verification

- `cargo build` / `cargo test -p tikovm-protocol` / `-p tikovm-host` /
  `-p tikovm-guest` — all must build on macOS (stub backend) and Linux
  (Firecracker).
- Tests: codec/manifest round-trips; `MockVmm` + fake guestlink (mirroring
  `tikod/tests/db_routes.rs`); supervisor restart + health + idle-evaluator.
- `clippy` is safe on these crates (no `pgsys` FFI in the dependency set).
- Echo boot test modeled on `tikod/examples/boot_test.rs` (Linux + KVM only).

## 19. Implementation sequencing

1. Scaffold 3 crates + workspace registration + `tikovm-protocol` (types, codec,
   errors, state machine).
2. `tikovm-host`: `Vmm` trait + stub + port Firecracker (+vsock); `Node`
   (transition enforcement); control registry; config.
3. `tikovm-host`: `store.rs` (StateStore + SQLite, write-through) + boot
   reconciliation (restore-on-demand).
4. `tikovm-host`: networking (TAP/IPAM/vsock); storage (overlay + volume
   provisioner).
5. `tikovm-host`: proxy/router + guest proxy + guestlink + API + metrics.
6. `tikovm-host`: `scheduler.rs` (cron evaluation + keep-warm/ephemeral triggers).
7. `tikovm-guest`: manifest + supervisor + health + idle + hostlink/server +
   lifecycle + fs.
8. Echo rootfs + boot test.
9. Tests across all crates (incl. a hostd crash/restart recovery test).

## 20. Implementation status

What is **built and validated** vs. **designed but not yet built**. Validations
run on the dev host (Ubuntu 24.04 + KVM + Firecracker v1.17-dev); 34 unit tests
pass and `cargo clippy` is clean on all three new crates.

| Area | Status | Notes |
|---|---|---|
| `tikovm-protocol` (manifest, state machine, codec, routing, rpc, volumes) | ✅ built, unit-tested | |
| 13-state lifecycle (`Node`, transition enforcement) | ✅ built, unit-tested | `create/start/pause/resume/suspend/restore/destroy/freeze` |
| `Vmm` backends | ✅ `FirecrackerVmm` (Linux/KVM), `StubBackend` (non-Linux), `MockVmm` (tests) | overlay model + deterministic per-VM TAP/NAT networking |
| Crash recovery (`SqliteStore`, write-through, `reconcile`) | ✅ built, unit-tested + binary `kill -9` test | restore-on-demand policy |
| Scheduler (cron + interval, keep-warm/ephemeral) | ✅ built, unit-tested | ephemeral provisioning needs the full pipeline |
| Generic guest supervisor (restart policy + backoff + graceful stop) | ✅ built, unit-tested | |
| Idle evaluator (guest-authoritative scale-to-zero) | ✅ built, unit-tested | |
| **vsock control channel** (§7) | ✅ built, validated on KVM | guest→host: `GetNetworkStats`, `Suspend`. host→guest commands defined, not wired |
| **Scale-to-zero loop** (idle → suspend → wake-on-connect) | ✅ validated on KVM | restore ~0.35 s; the marquee serverless behavior |
| Control API (`api/server.rs`) + `tikovm-hostd` daemon | ✅ built, validated | `--mock` dev mode + real Firecracker |
| Echo workload rootfs | ✅ built, validated | `scripts/tikovm/build_echo_rootfs.sh` |
| TCP proxy (wake-on-connect data plane) | ✅ built, validated | single fixed target VM (multi-VM routing = next) |
| Workload volumes (`local_fast`, `remote_slow`) | 🟡 designed | `VolumeTier`/`VolumeDecl` types exist; provisioner not built. Firecracker has no virtio-fs → `remote_slow` via virtio-block-from-host-mount (fallback NFS-in-guest) |
| Host→guest commands (`PreSuspend`/`PostRestore` hooks) | 🟡 defined | for clean-snapshot quiesce; current freeze is abrupt |
| Multi-VM proxy routing (by `RoutingRule`: Host/path/header) | 🟡 designed | current proxy forwards to one configured VM |
| Prometheus metrics | 🟡 designed | tracing logs only today |
| vhost-user-block prod path for `remote_slow` | 🟡 future | |
| Multi-node clustering / distributed registry | 🟡 future | `StateStore` is the seam |
| PG / language-runtime / cron rootfs workloads | 🟡 future | the platform is workload-agnostic; concrete rootfs to follow |

**Verified on real microVMs:** provision → boot to `multi-user` → echo
reachable at the guest IP → idle → guest signals suspend over vsock → host
freezes (1 GB snapshot, RAM freed) → next connection wakes the VM
(restore-on-demand) → echo responds. Survives a hard `kill -9` of `hostd` with
SQLite recovery.
