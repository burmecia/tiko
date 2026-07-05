# Tiko Proxy Strategy: Peek-Then-Splice with VM Routing

Status: **design / agreed direction**
Owner: tikod
Last updated: 2026-07-05

---

## 1. TL;DR

Move the PostgreSQL proxy (`tikod/src/proxy/mod.rs`) from a **blind single-target
TCP forwarder** to a **"frame-aware during handshake, blind splice after
`ReadyForQuery`"** proxy. The proxy reads just enough of the PG wire startup to
extract a routing key — the `vm_id` — from the startup packet's `options` field
(`-c tiko.endpoint=<vm_id>`), looks up the target VM, performs wake-on-connect
(including true scale-from-zero), then splices raw bytes for the rest of the
connection. Auth is **passthrough** in v1 (the password is forwarded to the VM
untouched).

This mirrors the proven Neon-proxy design and is the minimal protocol awareness
that makes one listening port route to N VMs.

## 2. Background & current state

The proxy today (`proxy/mod.rs`) is a transparent TCP byte-pipe. It listens on
`:5432` and forwards every connection to a single hard-coded
`default_target` (`Direct("127.0.0.1:15432")`, see `main.rs` leaving
`ProxyConfig::default_target` at `Default::default()`). The `ForwardTarget::Vm`
variant and the wake-on-connect branch in `resolve_target` exist but are
**unreachable** at runtime — nothing ever constructs a `Vm` target per
connection.

Known gaps this strategy resolves:

1. **No per-connection routing** — one port, one fixed backend; the
   `tenant_id`/`branch_id` fields in `VmRecord` are invisible to the proxy.
2. **Wake-on-connect only handles `Paused`** — `Node::ensure_running`
   (`node/mod.rs:82`) returns `InvalidState` for `Stopped`, the very state a
   scaled-to-zero VM is in. The docstring overstates current capability.
3. **`resume_timeout_secs` is declared (`proxy/mod.rs:53`) but never enforced.**
4. **No `control.on_disconnect()` call** — `connection_count` would leak the
   moment VM routing is enabled, breaking any idle/auto-pause policy.
5. **Cancel requests would misroute** across multiple VMs in a blind proxy.
6. **Zero test coverage** of the proxy.

## 3. Goals & non-goals

**Goals**

- One listening port routes to N VMs, keyed by `vm_id`.
- Wake-on-connect covers **all** VM states: `Running`, `Paused`, and `Stopped`
  (true scale-from-zero) — closing the doc/reality gap.
- Concurrent clients to a cold VM get a single coordinated restore, not N
  racing restores (single-flight, owned by `Node`/`Control`).
- Cancel requests (`CancelRequest`) reach the correct backend VM.
- `resume_timeout_secs` is enforced; clients get a PG-formatted error on
  timeout, not a bare TCP close.
- `connection_count` stays accurate (`on_disconnect` wired in).

**Non-goals (v1)**

- **No TLS termination.** The proxy replies `'N'` to `SSLRequest` (plaintext to
  the proxy). `sslmode=require` clients cannot connect through the proxy until a
  future phase adds TLS + SNI routing. Explicitly scoped out.
- **No edge auth / JWT validation** — passthrough only.
- **No connection pooling / transaction-level multiplexing** (future; requires a
  full protocol layer).
- **No query-level inspection.**

## 4. Architecture

```text
Client ──TCP──→ Proxy (:5432)
                 │
                 │  1. read first msg:
                 │       SSLRequest / GSSENCRequest → reply 'N', continue
                 │       CancelRequest (magic 80877102) → cancel path (§8)
                 │  2. read StartupMessage, buffer bytes
                 │  3. parse options → tiko.endpoint=<vm_id>
                 │  4. vm_id ──lookup──→ Control::get(&vm_id)
                 │       missing option or unknown vm_id → PG FATAL (§7)
                 │  5. Node::wake(vm_id, control):
                 │       Running  → forward
                 │       Paused   → resume
                 │       Stopped  → scale_from_zero(snapshot)  [single-flight]
                 │     wrapped in timeout(resume_timeout_secs)
                 │  6. connect backend (guest_ip:5432)
                 │  7. replay buffered startup bytes to backend
                 │  8. handshake-mode splice (both dirs) until ReadyForQuery 'Z'
                 │       └─ intercept BackendKeyData 'K' → {(pid,secret)→vm_id}
                 │  9. pure io::copy both dirs until close
                 │ 10. control.on_disconnect(vm_id)
                 ▼
              VM PG backend
```

The critical shift vs. today: **steps 1–3 and 8 parse**; everything else is the
existing blind copy. The parse surface is bounded to the handshake.

## 5. Routing key design

Connection string:

```
psql "host=... options=-c tiko.endpoint=<vm_id>"
# or, URL form:
postgres://...?options=-c%20tiko.endpoint%3D<vm_id>
```

- The PG `options` startup value is a single string of space-separated
  `-c key=value` tokens. The parser splits on whitespace, strips the `-c`
  prefix, and splits each on the first `=`.
- **The routing value is the `vm_id`** — used uniformly everywhere. In a future
  revision `vm_id` will be the user-facing, globally-unique `db_id` (encoded into
  the vm_id), so the same identifier serves as both the routing key and the
  user-visible database identity. No separate `endpoint_id` namespace is
  introduced.
- **Resolution** is a direct `Control::get(&vm_id)` (`control/mod.rs:124`). No
  reverse index is required because the key *is* the vm_id.
- **Reject hard.** Two failure cases both return a PG `FATAL` error (§7):
  - `tiko.endpoint` absent → `missing tiko.endpoint routing option`.
  - `tiko.endpoint=<vm_id>` present but vm_id unknown to the registry →
    `unknown VM <vm_id>`.
- `ProxyConfig.default_target` is retained **only as a dev/test escape hatch**
  (opt-in, for the existing `Direct` target used during local development). It is
  not consulted when `tiko.endpoint` is present, and production deployments
  should not rely on it.

## 6. Wake-on-connect & single-flight

Replace the `Vm` branch in `resolve_target` (`proxy/mod.rs:160`) with a call to a
unified `Node::wake`. Add to `node/mod.rs`:

```rust
pub async fn wake(&self, vm_id: &VmId, control: &Control) -> Result<(), VmError> {
    match self.vmm.vm_state(vm_id).await {
        Ok(VmState::Running) => Ok(()),
        Ok(VmState::Paused) => {
            self.vmm.resume_vm(vm_id).await?;
            Ok(())
        }
        Ok(VmState::Stopped) | Err(VmmError::NotFound) => {
            let snap = control.get_snapshot(vm_id)
                .ok_or(VmmError::NoSnapshot { vm_id: vm_id.clone() })?;
            self.scale_from_zero(&snap).await?;   // restore + resume
            Ok(())
        }
        other => Err(VmmError::InvalidState { /* ... */ }),
    }
}
```

This fixes the current bug where `ensure_running` (`node/mod.rs:82`) returns
`InvalidState` for the very `Stopped` state the proxy advertises it can wake.

**Single-flight ownership: `Node`/`Control` (not the proxy).** Both the proxy
and the HTTP `PUT /vms/{vm_id}/scale-from-zero` route call `Node::wake`, so both
must share one in-flight guard. The coordinator lives in `Control` (the shared
state plane) so the two planes agree:

```rust
// control/mod.rs
restores: DashMap<VmId, Arc<RestoreCoord>>,
```

where `RestoreCoord` carries a `Notify` plus a result slot. `Node::wake`
consults `Control`:

- If an in-flight restore exists for `vm_id`, attach to its `Notify` and await.
- Otherwise register itself as the leader, run `scale_from_zero`, store the
  result, `notify_waiters()`, and remove the entry.

The HTTP restore route delegates to the same `Node::wake`, so it gets
single-flight for free and cannot race the proxy.

`vm_id` is stable across scale-to-zero cycles because `Node::scale_from_zero`
returns `snapshot.vm_id` (`node/mod.rs:71,73`), so the mapping remains valid
after restore.

## 7. Timeout & error handling

`Node::wake` is wrapped in:

```rust
tokio::time::timeout(Duration::from_secs(config.resume_timeout_secs), node.wake(...))
```

(the field already exists at `proxy/mod.rs:53`; it is currently unused). All
routing/wake failures are reported to the client as a PG **`ErrorResponse`**
(type byte `'E'`, fields `S`, `V`, `C`, `M`) so libpq prints a clean message
instead of seeing a bare TCP reset:

| Condition | Severity | SQLSTATE | Message |
|---|---|---|---|
| `tiko.endpoint` option missing | FATAL | 28000 | `missing tiko.endpoint routing option` |
| Unknown vm_id (not in registry) | FATAL | 28000 | `unknown VM <vm_id>` |
| VM in unusable state (e.g. `Snapshotting`) | FATAL | 08006 | `VM <vm_id> is in state <S>, cannot forward` |
| Wake timeout (`resume_timeout_secs`) | FATAL | 08006 | `VM <vm_id> did not start within <N>s` |
| Stopped VM with no stored snapshot | FATAL | 08006 | `VM <vm_id> has no snapshot; cannot restore` |

After sending the error packet, the proxy closes the connection.

## 8. Cancel-request routing

A blind multi-VM proxy **silently misroutes cancels**. `psql` Ctrl-C opens a
*new* TCP connection and sends `CancelRequest` (magic `80877102`, 16 bytes:
`len | code | pid | secret`) carrying the PID + secret from the original
connection's `BackendKeyData`. Two-part fix:

1. During handshake-mode splice (step 8), scan the backend→client stream for the
   `BackendKeyData` message (type byte `'K'`, body: `len(4) | pid(4) |
   secret(4)`). Insert `({pid, secret}) → {vm_id, backend_addr}` into a
   proxy-local table with per-entry TTL/eviction (entries become stale when the
   owning connection closes).
2. On a new connection whose first message is `CancelRequest`, look up the table
   by `(pid, secret)`, forward the cancel bytes to that backend, and close
   immediately (cancel connections are write-only).

In scope for v1: without it, `Ctrl-C` targets the wrong VM — a visible, confusing
bug once multiple VMs share the port.

## 9. Handshake-boundary parsing (why this stays simple)

The proxy parses **only** until it sees `ReadyForQuery` (type `'Z'`) from the
backend, then flips to pure `io::copy`. This bounds all protocol-awareness to a
small, well-understood window (startup + auth + parameter status). After `'Z'`,
the code is identical to today's `try_join!` of two copies
(`proxy/mod.rs:134`). The startup packet itself is ~50 LoC to parse by hand
(4-byte length, 4-byte protocol version, then NUL-terminated `key\0value\0`
pairs until an empty key) — **no new crate dependency is required**.

## 10. State / registry model

Minimal changes, because the routing key *is* the vm_id:

- **`Control`** gains a single-flight restore coordinator
  (`restores: DashMap<VmId, Arc<RestoreCoord>>`) plus accessors. No reverse
  endpoint index is needed — lookup is the existing `Control::get(&vm_id)`.
- **`Control::register`** should stop silently defaulting `tenant_id` /
  `branch_id` to empty strings (`api/server.rs:384`); at minimum log a warning,
  since identity will matter for policy.
- The `POST /vms/{vm_id}/register` route does **not** need a new field — `vm_id`
  is already the path parameter and is the routing key.

## 11. Phased roadmap

| Phase | Scope | Deliverable |
|---|---|---|
| **0 — Correctness** | Reachable wake-on-connect; `Node::wake` covering `Stopped`; single-flight restore in `Control`; enforce `resume_timeout_secs`; wire `on_disconnect`; PG-formatted error packets | No protocol change; today's routing still works. Fixes the doc/reality gap and the dormant-code issue. Independently shippable. |
| **1 — VM routing** | Startup packet parser (`proxy/startup.rs`); extract `tiko.endpoint=<vm_id>`; lookup via `Control::get`; reject hard on missing/unknown; replay buffered startup to backend; handshake→splice boundary; `on_disconnect` | One port routes to many VMs. |
| **2 — Cancel routing** | `BackendKeyData` interception; `CancelRequest` path; TTL eviction | Correct `psql` Ctrl-C across VMs. |
| **3 — Hardening (future)** | TLS termination (reply `'S'`, route on SNI too); edge auth/JWT; connection pooling (Supavisor-style); graceful shutdown draining | Production security/scale. |

## 12. File-level change map

| File | Change |
|---|---|
| `tikod/src/proxy/mod.rs` | Rewrite `handle_connection` to peek → resolve → wake → replay → splice; remove the dead `Vm` match in `resolve_target`; call `on_disconnect` on close |
| `tikod/src/proxy/startup.rs` *(new)* | `read_startup(stream) -> Result<StartupInfo>`: handle `SSLRequest`/`GSSENCRequest`/`CancelRequest` dispatch; parse `StartupMessage` pairs; extract `options` |
| `tikod/src/proxy/cancel.rs` *(new, Phase 2)* | `CancelTable: DashMap<(Pid, Secret), (VmId, BackendAddr)>` + intercept + route |
| `tikod/src/proxy/error.rs` *(new)* | Build PG `ErrorResponse` bytes for the cases in §7 |
| `tikod/src/control/mod.rs` | `restores: DashMap<VmId, Arc<RestoreCoord>>` + single-flight accessors |
| `tikod/src/node/mod.rs` | `Node::wake(vm_id, control)` unifying `Running`/`Paused`/`Stopped`; consults `Control` single-flight |
| `tikod/src/api/server.rs` | HTTP `scale-from-zero`/`restore` routes delegate to `Node::wake` (share single-flight with proxy) |
| `tikod/src/main.rs` | No change — `default_target` retained as dev-only fallback |
| `tikod/Cargo.toml` | No new deps for Phases 0–2 (hand-rolled parser). Revisit if Phase 3 adds TLS/auth. |

## 13. Testing strategy (currently zero coverage)

- **Unit** (`proxy/startup.rs`): golden bytes for `SSLRequest`,
  `GSSENCRequest`, `CancelRequest`, `StartupMessage` with `options`, and
  malformed packets. Error-packet construction (`error.rs`).
- **Integration** (`tikod/tests/`): spin `Proxy` + a mock PG backend that
  completes the handshake; assert (a) routing by `tiko.endpoint=<vm_id>`,
  (b) reject-hard on missing/unknown vm_id, (c) buffered startup bytes replayed
  verbatim, (d) `connection_count` returns to 0 after disconnect, (e) timeout
  produces a PG error packet.
- **Wake**: a fake `Vmm` returning `Stopped` + a stored snapshot → assert
  `scale_from_zero` invoked exactly once under N concurrent connections
  (single-flight).
- **Cancel**: two connections with distinct PIDs → assert a cancel for pid A
  routes to backend A, not B.

## 14. Decisions log

| # | Question | Decision |
|---|---|---|
| 1 | Routing key namespace | Use **`vm_id`** everywhere. Future `vm_id` will be the user-facing, globally-unique `db_id` (encoded), serving as both routing key and DB identity. No separate `endpoint_id`. |
| 2 | Single-flight ownership | **`Node`/`Control`** — so the proxy and the HTTP `scale-from-zero` route share one in-flight guard. |
| 3 | TLS | **Scoped out** for v1. Proxy replies `'N'` to `SSLRequest`. Future phase adds TLS + SNI routing. |
| 4 | Unknown-endpoint policy | **Reject hard** — PG `FATAL` for both missing `tiko.endpoint` and unknown `vm_id`. `default_target` retained only as a dev/test escape hatch. |
| 5 | Stopped VM with no snapshot | **PG `FATAL`** with a clear message (`VM <id> has no snapshot; cannot restore`). |

## 15. Future work (explicitly out of scope for v1)

- TLS termination in the proxy, enabling SNI-based routing
  (`ep-xxx.tiko.example.com`) as an alternative/complement to the `options`
  field.
- Edge authentication (validate a JWT / API key in the password field before
  spending seconds on a restore; refuse unknown tenants cold).
- Connection pooling / transaction-level multiplexing (Supavisor / PgBouncer
  style) to reduce wake frequency and VM resource use.
- Graceful shutdown draining (in-flight connections on `SIGTERM`).
