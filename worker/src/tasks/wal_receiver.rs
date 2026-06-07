//! WAL receiver — uploads WAL to the sim/S3 store in near-realtime.
//!
//! Connects to the local postmaster via the **PostgreSQL physical streaming
//! replication protocol** over a Unix socket.  Accumulates WAL bytes in a
//! per-segment in-memory buffer (up to 16 MiB) and uploads 256 KiB chunk
//! objects as data arrives.  On segment switch the full buffer is zero-padded
//! and PUT as a sealed segment object; chunks are then deleted (compaction).
//!
//! `tokio-postgres` 0.7 does not expose `CopyBoth` mode (needed for physical
//! replication), so this module implements the minimal PostgreSQL wire
//! protocol directly over `tokio::net::UnixStream`.
//!
//! See `wal_streaming.md` for the design rationale.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use pgsys::timeline_id::TimelineId;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::log_relay::{relay_debug1, relay_warning};
use core::io::store::Store;
use core::io_control::IoControl;
use pgsys::common::XLOG_SEG_SIZE;
use std::sync::atomic::Ordering;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Chunk upload size (256 KiB — fixed, not configurable).
const CHUNK_BYTES: usize = 256 * 1024;

/// Microsecond offset from Unix epoch (1970) to PostgreSQL epoch (2000-01-01).
const PG_EPOCH_OFFSET_US: i64 = 946_684_800 * 1_000_000;

/// How long to wait for a WAL message before sending a proactive keepalive.
/// Must be well under `wal_sender_timeout` (default 60 s).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

// ── Config ────────────────────────────────────────────────────────────────────

const DEFAULT_CONNSTR: &str = "host=/tmp port=5432 dbname=postgres replication=true";
const DEFAULT_SLOT_NAME: &str = "tiko_wal_stream";

pub struct WalReceiverConfig {
    /// libpq-style connstring: `host=/tmp port=5432 dbname=postgres replication=true`
    pub connstr: &'static str,
    /// Physical replication slot name.
    pub slot_name: &'static str,
}

impl Default for WalReceiverConfig {
    fn default() -> Self {
        WalReceiverConfig {
            connstr: DEFAULT_CONNSTR,
            slot_name: DEFAULT_SLOT_NAME,
        }
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Tokio task: stream WAL from the local primary to the sim store.
///
/// Never returns.  Reconnects with exponential backoff on any error.
pub async fn wal_receiver_task(sim: &'static Store, config: WalReceiverConfig) {
    relay_debug1(format!(
        "tiko: wal_receiver: task started with connstr: {}, slot: {}",
        config.connstr, config.slot_name
    ));
    let mut backoff = Duration::from_secs(1);
    loop {
        sleep(backoff).await;
        match run_streaming(sim, &config).await {
            Ok(()) => {
                relay_debug1("tiko: wal_receiver: connection closed, reconnecting");
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                backoff = (backoff * 2).min(Duration::from_secs(60));
                relay_warning(format!(
                    "tiko: wal_receiver: {e}, reconnecting in {backoff:?}"
                ));
            }
        }
    }
}

// ── Per-segment in-memory state ───────────────────────────────────────────────

struct SegState {
    seg_no: u64,
    /// Raw WAL bytes received for this segment. Grows up to XLOG_SEG_SIZE.
    buf: Vec<u8>,
    /// Bytes already covered by chunk PUTs (multiple of CHUNK_BYTES).
    chunks_uploaded: usize,
    /// In-flight chunk PUT tasks joined before sealing.
    chunk_tasks: JoinSet<Result<(), String>>,
    /// True when streaming began partway into this segment, so `buf[0..offset)`
    /// is a zero-filled placeholder rather than real WAL. Such a segment must
    /// never be written as a sealed (complete) object — see `seal_segment`.
    partial: bool,
}

impl SegState {
    fn new(seg_no: u64) -> Self {
        SegState {
            seg_no,
            buf: Vec::with_capacity(XLOG_SEG_SIZE),
            chunks_uploaded: 0,
            chunk_tasks: JoinSet::new(),
            partial: false,
        }
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ── Core streaming loop ───────────────────────────────────────────────────────

async fn run_streaming(sim: &'static Store, config: &WalReceiverConfig) -> Result<(), BoxError> {
    let params = parse_connstr(&config.connstr);

    // ── Connect ───────────────────────────────────────────────────────────────
    let socket_path = unix_socket_path(&params)?;
    // Fall back to the OS user (same default as libpq) rather than hardcoding
    // "postgres", which may not exist on the local system.
    let os_user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_string());
    let user = params.get("user").copied().unwrap_or(os_user.as_str());
    let mut conn = ReplConn::connect(&socket_path, user).await?;
    relay_debug1("tiko: wal_receiver: connected to postmaster");

    // ── IDENTIFY_SYSTEM → timeline + current xlogpos ─────────────────────────
    // Columns: systemid | timeline | xlogpos | dbname
    let rows = conn.simple_query("IDENTIFY_SYSTEM").await?;
    let row = rows.first().ok_or("IDENTIFY_SYSTEM: empty response")?;
    let timeline_id: TimelineId = row
        .get(1)
        .and_then(|s| s.as_deref())
        .ok_or("IDENTIFY_SYSTEM: missing timeline")?
        .parse::<u32>()
        .map_err(|e| format!("IDENTIFY_SYSTEM: bad timeline: {e}"))?
        .into();
    // xlogpos is the fallback start LSN when the slot has no restart_lsn yet
    // (slot created without RESERVE_WAL, or never used).
    let xlogpos_str = row
        .get(2)
        .and_then(|s| s.clone())
        .ok_or("IDENTIFY_SYSTEM: missing xlogpos")?;
    let xlogpos =
        parse_lsn(&xlogpos_str).map_err(|e| format!("IDENTIFY_SYSTEM: bad xlogpos: {e}"))?;

    // ── Ensure slot exists and get restart_lsn ────────────────────────────────
    // Try READ_REPLICATION_SLOT first: succeeds silently if the slot already
    // exists (common case on every restart after the first), avoiding the
    // server-side ERROR that CREATE would log for a duplicate slot.
    // Only fall back to CREATE when READ reports the slot is missing.
    // READ_REPLICATION_SLOT always returns exactly one row.
    // When the slot does not exist, all columns are NULL (slot_type = NULL).
    // When it exists, slot_type is non-NULL; restart_lsn may still be NULL
    // if the slot was never used or created without RESERVE_WAL.
    //
    //   slot_type=NULL              → slot absent → CREATE, start from xlogpos
    //   slot_type set, restart_lsn set  → normal resume from restart_lsn
    //   slot_type set, restart_lsn NULL → start from xlogpos
    let read_sql = format!("READ_REPLICATION_SLOT {}", config.slot_name);
    let read_rows = conn.simple_query(&read_sql).await?;
    let row = read_rows.into_iter().next().unwrap_or_default();
    let slot_type = row.first().and_then(|v| v.as_deref()); // NULL ↔ slot absent
    let start_lsn = if slot_type.is_none() {
        // All-NULL row — slot does not exist.  Create it.
        // RESERVE_WAL retains WAL from this instant, before START_REPLICATION.
        // CREATE returns (slot_name, consistent_point, snapshot, plugin).
        let create_sql = format!(
            "CREATE_REPLICATION_SLOT {} PHYSICAL RESERVE_WAL",
            config.slot_name
        );
        conn.simple_query(&create_sql).await?;
        relay_debug1(format!(
            "tiko: wal_receiver: created slot '{}', starting from {xlogpos_str}",
            config.slot_name
        ));
        // consistent_point in the CREATE response is always 0/0 for physical
        // slots — meaningless.  RESERVE_WAL retains WAL from xlogpos onwards.
        xlogpos
    } else {
        // Slot exists — column 1 is restart_lsn (may be NULL if slot has never streamed).
        match row.into_iter().nth(1).flatten() {
            Some(s) => {
                relay_debug1(format!(
                    "tiko: wal_receiver: slot '{}' already exists, resuming",
                    config.slot_name
                ));
                parse_lsn(&s).map_err(|e| format!("slot restart_lsn parse error: {e}"))?
            }
            None => {
                // restart_lsn is NULL — slot was not created with RESERVE_WAL
                // or has never streamed.  Start from current WAL position.
                relay_debug1(format!(
                    "tiko: wal_receiver: slot '{}' has no restart_lsn, starting from current WAL position ({xlogpos_str})",
                    config.slot_name
                ));
                xlogpos
            }
        }
    };

    // ── START_REPLICATION ─────────────────────────────────────────────────────
    let start_sql = format!(
        "START_REPLICATION SLOT {} PHYSICAL {:X}/{:X} TIMELINE {}",
        config.slot_name,
        (start_lsn >> 32) as u32,
        start_lsn as u32,
        timeline_id.as_u32()
    );
    conn.start_replication(&start_sql).await?;
    relay_debug1(format!(
        "tiko: wal_receiver: streaming started (slot={}, tl={}, lsn={:X}/{:X})",
        config.slot_name,
        timeline_id.to_hex_variable_width(),
        (start_lsn >> 32) as u32,
        start_lsn as u32,
    ));

    // ── Message loop ──────────────────────────────────────────────────────────
    let mut confirmed_lsn: u64 = start_lsn;
    let mut cur_seg: Option<SegState> = None;
    // Last checkpoint/commit generation we flushed the tail at. Initialized to
    // the current value so we don't flush spuriously right after connecting.
    let mut last_flushed_generation: u64 = current_generation();

    loop {
        // Wait up to KEEPALIVE_INTERVAL for the next CopyData message.
        // On timeout, send a proactive StandbyStatusUpdate so walsender does
        // not close the connection via wal_sender_timeout (default 60 s).
        match tokio::time::timeout(KEEPALIVE_INTERVAL, conn.recv_copy_data()).await {
            Ok(Ok(Some(msg))) => {
                if msg.is_empty() {
                    continue;
                }
                match msg[0] {
                    b'w' => {
                        handle_xlogdata(
                            &msg,
                            sim,
                            timeline_id,
                            &mut cur_seg,
                            &mut confirmed_lsn,
                            &mut conn,
                        )
                        .await?;
                    }
                    b'k' => {
                        // PrimaryKeepalive: [k][end_lsn(8)][time(8)][reply(1)]
                        if msg.len() >= 18 && msg[17] != 0 {
                            send_standby_status(&mut conn, confirmed_lsn).await?;
                        }
                    }
                    _ => {
                        relay_warning(format!(
                            "tiko: wal_receiver: unknown CopyData type 0x{:02X}",
                            msg[0]
                        ));
                    }
                }
            }
            Ok(Ok(None)) => {
                // CopyDone: walsender finished streaming through the shutdown
                // checkpoint. Flush the partial tail so S3 has WAL up to the
                // actual shutdown point, not just the last sealed segment.
                flush_partial_tail(&mut cur_seg, sim, timeline_id).await?;
                return Ok(());
            }
            Ok(Err(e)) => return Err(e),
            Err(_timeout) => {
                // No message — send proactive keepalive.
                send_standby_status(&mut conn, confirmed_lsn).await?;
            }
        }

        // Checkpoint-triggered tail flush: if a checkpoint advanced the shared
        // generation since our last flush, push whatever WAL tail we've buffered
        // so archived WAL tracks the head. Cheap atomic read on a hit-or-miss
        // basis; the actual PUT only fires when there is a tail.
        maybe_flush_on_checkpoint(&cur_seg, sim, timeline_id, &mut last_flushed_generation)
            .await?;
    }
}

// ── XLogData ingestion ────────────────────────────────────────────────────────

/// Process one `XLogData` ('w') message.
///
/// Wire format: `[w(1)][start_lsn(8)][end_lsn(8)][server_time(8)][wal_data...]`
///
/// Detects segment switches, appends WAL bytes, fires chunk PUTs.
#[allow(clippy::too_many_arguments)]
async fn handle_xlogdata(
    msg: &[u8],
    sim: &'static Store,
    //ns: &ProjectNamespace,
    timeline_id: TimelineId,
    cur_seg: &mut Option<SegState>,
    confirmed_lsn: &mut u64,
    conn: &mut ReplConn,
) -> Result<(), BoxError> {
    if msg.len() < 25 {
        return Ok(());
    }
    let start_lsn = u64::from_be_bytes(msg[1..9].try_into().unwrap());
    let wal_data = &msg[25..];
    if wal_data.is_empty() {
        return Ok(());
    }

    let seg_no_new = start_lsn / XLOG_SEG_SIZE as u64;

    // Detect segment switch.
    // The walsender never sends a single XLogData message that crosses a
    // segment boundary, so all of `wal_data` belongs to `seg_no_new`.
    if let Some(state) = cur_seg.as_ref() {
        if state.seg_no != seg_no_new {
            let old = cur_seg.take().unwrap();
            seal_segment(old, sim, timeline_id, confirmed_lsn, conn).await?;
        }
    }

    if cur_seg.is_none() {
        let seg_offset = (start_lsn % XLOG_SEG_SIZE as u64) as usize;
        let mut s = SegState::new(seg_no_new);
        if seg_offset > 0 {
            // Zero-fill up to the actual data offset so the sealed segment
            // object has WAL bytes at the correct positions within the file.
            // This happens only on the very first connection when the slot's
            // restart_lsn is mid-segment (xlogpos at slot creation time).
            // On reconnects, confirmed_lsn is always segment-aligned so
            // streaming resumes at offset 0.
            s.buf.resize(seg_offset, 0);
            // Skip chunk uploads for the zero-filled prefix — it is not real
            // WAL. Mark the segment partial so it is never sealed as a complete
            // object: the prefix is zeros, and a sealed object would masquerade
            // as a fully-valid 16 MiB segment that restore could replay into.
            s.chunks_uploaded = seg_offset;
            s.partial = true;
        }
        *cur_seg = Some(s);
    }
    let state = cur_seg.as_mut().unwrap();
    state.buf.extend_from_slice(wal_data);

    // Fire chunk PUTs for newly complete 256 KiB windows — non-blocking.
    while state.buf.len() - state.chunks_uploaded >= CHUNK_BYTES {
        let offset = state.chunks_uploaded;
        let slice = state.buf[offset..offset + CHUNK_BYTES].to_vec();
        let name = wal_seg_name(timeline_id, state.seg_no);
        let key = sim.locator().wal_chunk_key(timeline_id, &name, offset);
        state.chunk_tasks.spawn(async move {
            tokio::task::spawn_blocking(move || sim.storage_put(&key, &slice))
                .await
                .map_err(|e| format!("chunk task panicked: {e}"))?
                .map_err(|e| format!("chunk PUT failed: {e}"))
        });
        state.chunks_uploaded += CHUNK_BYTES;
    }

    Ok(())
}

// ── Segment sealing ───────────────────────────────────────────────────────────

/// Seal a completed WAL segment.
///
/// 1. Join all in-flight chunk PUTs and upload the tail (any error propagates
///    to the reconnect loop).
/// 2. For a fully-observed segment: zero-pad to `XLOG_SEG_SIZE`, PUT the sealed
///    object, and compact (delete) the now-superseded chunk objects.
///    For a partial segment (streaming began mid-segment): keep chunks only —
///    its `[0, start_offset)` prefix is a zero placeholder, so a sealed object
///    would falsely advertise a complete, replayable 16 MiB segment.
/// 3. Advance `confirmed_lsn` — the ONLY place it moves — and send a
///    `StandbyStatusUpdate`, always at a segment boundary.
async fn seal_segment(
    mut state: SegState,
    sim: &'static Store,
    //ns: &ProjectNamespace,
    timeline_id: TimelineId,
    confirmed_lsn: &mut u64,
    conn: &mut ReplConn,
) -> Result<(), BoxError> {
    let seg_no = state.seg_no;
    let name = wal_seg_name(timeline_id, seg_no);

    // 1. Drain inflight chunk PUTs and flush the trailing partial chunk.
    join_chunks_and_flush_tail(&mut state, sim, timeline_id, &name).await?;

    if state.partial {
        // Mid-stream start: do NOT write a sealed object and do NOT compact —
        // the chunks covering [start_offset, end) are the only real copy.
        relay_debug1(format!(
            "tiko: wal_receiver: segment {name} began mid-stream; kept as chunks-only (no sealed object)"
        ));
    } else {
        // 2. Zero-pad to XLOG_SEG_SIZE and PUT the sealed segment.
        state.buf.resize(XLOG_SEG_SIZE, 0);
        let sealed = state.buf;
        let seg_key = sim.locator().wal_segment(timeline_id, &name);
        let name_log = name.clone();
        tokio::task::spawn_blocking(move || sim.storage_put(&seg_key, &sealed))
            .await
            .map_err(|e| format!("seal spawn_blocking panicked: {e}"))?
            .map_err(|e| format!("sealed PUT failed for {name_log}: {e}"))?;

        relay_debug1(format!("tiko: wal_receiver: sealed {name}"));

        // Best-effort compaction: delete chunk objects (fire-and-forget).
        // Stranded chunks are harmless — tiko_restore prefers the sealed object.
        let chunk_prefix = sim.locator().wal_chunk_prefix(timeline_id, &name);
        tokio::task::spawn_blocking(move || {
            let _ = sim.storage_delete(&chunk_prefix);
        });
    }

    // 3. Advance confirmed_lsn — only here, always at a segment boundary.
    //    Must stay segment-aligned: reconnect resumes from this value, and a
    //    mid-segment value would force the partial-start path on every resume.
    *confirmed_lsn = (seg_no + 1) * XLOG_SEG_SIZE as u64;
    send_standby_status(conn, *confirmed_lsn).await?;

    Ok(())
}

// ── Partial tail flush on clean shutdown ──────────────────────────────────────

/// On clean shutdown (CopyDone), upload any buffered bytes not yet covered by
/// a chunk PUT. Unlike `seal_segment`, does not zero-pad or write a sealed
/// segment object — the segment is incomplete and restore falls back to chunks.
async fn flush_partial_tail(
    cur_seg: &mut Option<SegState>,
    sim: &'static Store,
    timeline_id: TimelineId,
) -> Result<(), BoxError> {
    let mut state = match cur_seg.take() {
        Some(s) => s,
        None => return Ok(()),
    };
    let name = wal_seg_name(timeline_id, state.seg_no);
    join_chunks_and_flush_tail(&mut state, sim, timeline_id, &name).await?;
    relay_debug1(format!(
        "tiko: wal_receiver: flushed partial tail for {name}"
    ));
    Ok(())
}

/// Read the shared-memory checkpoint/commit generation. Returns 0 when
/// `IoControl` is not attached yet (very early startup) — treated as "no
/// checkpoint observed".
fn current_generation() -> u64 {
    IoControl::try_get()
        .map(|c| c.timeline.generation.load(Ordering::Acquire))
        .unwrap_or(0)
}

/// Checkpoint-triggered tail flush: if the shared-memory `generation` advanced
/// since `last_flushed_generation` and the current segment has buffered tail,
/// PUT that tail now. Always advances `last_flushed_generation` to the observed
/// value so an unchanged generation isn't re-evaluated.
async fn maybe_flush_on_checkpoint(
    cur_seg: &Option<SegState>,
    sim: &'static Store,
    timeline_id: TimelineId,
    last_flushed_generation: &mut u64,
) -> Result<(), BoxError> {
    let cur = current_generation();
    let has_tail = cur_seg
        .as_ref()
        .is_some_and(|s| s.buf.len() > s.chunks_uploaded);
    if should_flush_tail(*last_flushed_generation, cur, has_tail) {
        // has_tail implies cur_seg is Some.
        flush_tail_now(cur_seg.as_ref().unwrap(), sim, timeline_id).await?;
    }
    *last_flushed_generation = cur;
    Ok(())
}

/// PUT the current partial tail (`buf[chunks_uploaded..]`) at its byte offset,
/// WITHOUT consuming the segment or advancing `chunks_uploaded`.
///
/// Used by the checkpoint-triggered flush so archived WAL keeps up with the
/// head. Because `chunks_uploaded` is unchanged, the streaming path still owns
/// that offset: when the buffer later fills a full 256 KiB window (or the
/// segment is sealed), the normal PUT at the same key overwrites this partial
/// object. `tiko_restore` takes the latest object per offset, so reads stay
/// consistent. No-op if there is no buffered tail.
async fn flush_tail_now(
    state: &SegState,
    sim: &'static Store,
    timeline_id: TimelineId,
) -> Result<(), BoxError> {
    if state.buf.len() <= state.chunks_uploaded {
        return Ok(());
    }
    let name = wal_seg_name(timeline_id, state.seg_no);
    let offset = state.chunks_uploaded;
    let tail = state.buf[offset..].to_vec();
    let key = sim.locator().wal_chunk_key(timeline_id, &name, offset);
    tokio::task::spawn_blocking(move || sim.storage_put(&key, &tail))
        .await
        .map_err(|e| format!("checkpoint tail flush spawn_blocking panicked for {name}: {e}"))?
        .map_err(|e| format!("checkpoint tail PUT failed for {name}: {e}"))?;
    Ok(())
}

/// Join all in-flight chunk PUTs, then upload any buffered tail bytes not yet
/// covered by a full chunk. Shared by `seal_segment` and `flush_partial_tail`.
///
/// The tail is keyed at `chunks_uploaded` (its real byte offset within the
/// segment), so chunk keys remain consistent whether or not a sealed object
/// is later written.
async fn join_chunks_and_flush_tail(
    state: &mut SegState,
    sim: &'static Store,
    timeline_id: TimelineId,
    name: &str,
) -> Result<(), BoxError> {
    while let Some(result) = state.chunk_tasks.join_next().await {
        result.map_err(|e| format!("chunk task panicked for {name}: {e}"))??;
    }

    let chunks_uploaded = state.chunks_uploaded;
    if state.buf.len() > chunks_uploaded {
        let tail = state.buf[chunks_uploaded..].to_vec();
        let tail_key = sim
            .locator()
            .wal_chunk_key(timeline_id, name, chunks_uploaded);
        tokio::task::spawn_blocking(move || sim.storage_put(&tail_key, &tail))
            .await
            .map_err(|e| format!("tail spawn_blocking panicked for {name}: {e}"))?
            .map_err(|e| format!("tail PUT failed for {name}: {e}"))?;
    }

    Ok(())
}

// ── StandbyStatusUpdate ───────────────────────────────────────────────────────

/// Send a `StandbyStatusUpdate` ('r') to advance the slot's `restart_lsn`.
///
/// Wire format: `[r][write_lsn(8)][flush_lsn(8)][apply_lsn(8)][time(8)][reply(1)]`
async fn send_standby_status(conn: &mut ReplConn, flush_lsn: u64) -> Result<(), BoxError> {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
        - PG_EPOCH_OFFSET_US;

    let mut buf = BytesMut::with_capacity(34);
    buf.put_u8(b'r');
    buf.put_u64(flush_lsn); // write_lsn
    buf.put_u64(flush_lsn); // flush_lsn — server uses this to advance slot
    buf.put_u64(flush_lsn); // apply_lsn
    buf.put_i64(now_us); // client_time (μs since PG epoch 2000-01-01)
    buf.put_u8(0); // reply_requested = false

    conn.send_copy_data(&buf).await
}

/// Decide whether the checkpoint-triggered path should flush the WAL tail now.
///
/// Flush only when the shared-memory `generation` advanced since our last flush
/// (a checkpoint — or compaction — committed) AND there is buffered tail to push.
fn should_flush_tail(last_gen: u64, cur_gen: u64, has_tail: bool) -> bool {
    cur_gen != last_gen && has_tail
}

// ── Utility ───────────────────────────────────────────────────────────────────

/// Format a WAL segment filename from timeline and segment number.
/// Format: `{timeline:08X}{seg_no:016X}`  e.g. `000000010000000000000002`
fn wal_seg_name(timeline_id: TimelineId, seg_no: u64) -> String {
    format!("{}{:016X}", timeline_id.to_hex(), seg_no)
}

// ══════════════════════════════════════════════════════════════════════════════
// Raw PostgreSQL wire protocol client (minimal, for physical replication only)
// ══════════════════════════════════════════════════════════════════════════════
//
// tokio-postgres 0.7 does not expose CopyBoth mode, which is required for
// physical replication (walsender uses bidirectional COPY).  This section
// implements only what is needed:
//   - Startup + trust authentication
//   - Simple-query protocol (IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT)
//   - CopyBoth mode (START_REPLICATION + send/recv CopyData)
//
// Authentication: trust is assumed for local Unix socket connections.
// Clear-text and MD5 password are also handled.  SCRAM-SHA-256 is not — for
// SCRAM, configure pg_hba.conf with `trust` for `local replication`.

struct ReplConn {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl ReplConn {
    /// Connect to the PostgreSQL Unix socket and complete startup/auth.
    async fn connect(socket_path: &str, user: &str) -> Result<Self, BoxError> {
        let stream = UnixStream::connect(socket_path)
            .await
            .map_err(|e| format!("cannot connect to PostgreSQL socket {socket_path}: {e}"))?;
        let (read, write) = stream.into_split();
        let mut conn = ReplConn {
            reader: BufReader::new(read),
            writer: write,
        };
        conn.send_startup(user).await?;
        conn.handle_auth(user).await?;
        conn.drain_until_ready().await?;
        Ok(conn)
    }

    /// Send the startup message (PostgreSQL protocol v3, `replication=true`).
    async fn send_startup(&mut self, user: &str) -> Result<(), BoxError> {
        let mut body = BytesMut::new();
        body.put_u32(196608u32); // protocol version 3.0 (3 << 16 | 0)
        for (k, v) in &[
            ("user", user),
            ("replication", "true"),
            ("application_name", DEFAULT_SLOT_NAME),
        ] {
            body.put(k.as_bytes());
            body.put_u8(0);
            body.put(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0); // parameter list terminator
        let mut msg = BytesMut::with_capacity(4 + body.len());
        msg.put_u32((4 + body.len()) as u32); // length includes itself
        msg.put(body);
        self.writer.write_all(&msg).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Handle authentication exchange after the startup message.
    async fn handle_auth(&mut self, _user: &str) -> Result<(), BoxError> {
        let (msg_type, data) = self.read_message().await?;
        match msg_type {
            b'R' => {
                if data.len() < 4 {
                    return Err("auth message too short".into());
                }
                let auth_type = u32::from_be_bytes(data[0..4].try_into().unwrap());
                match auth_type {
                    0 => Ok(()), // AuthenticationOk — trust auth
                    3 => {
                        // AuthenticationCleartextPassword
                        Err("PostgreSQL requested cleartext password; configure pg_hba.conf with 'local replication all trust'".into())
                    }
                    5 => {
                        // AuthenticationMD5Password
                        Err("PostgreSQL requested MD5 password; configure pg_hba.conf with 'local replication all trust'".into())
                    }
                    10 => {
                        // AuthenticationSASL (SCRAM)
                        Err("PostgreSQL requested SCRAM auth; configure pg_hba.conf with 'local replication all trust'".into())
                    }
                    _ => Err(format!(
                        "unsupported auth type {auth_type}; configure pg_hba.conf with 'local replication all trust'"
                    ).into()),
                }
            }
            b'E' => Err(parse_error_response(&data).into()),
            _ => Err(
                format!("unexpected message type 0x{msg_type:02X} during authentication").into(),
            ),
        }
    }

    /// Read and discard messages until `ReadyForQuery` ('Z').
    async fn drain_until_ready(&mut self) -> Result<(), BoxError> {
        loop {
            let (msg_type, data) = self.read_message().await?;
            match msg_type {
                b'Z' => return Ok(()), // ReadyForQuery
                b'K' | b'S' => {}      // BackendKeyData, ParameterStatus — ignore
                b'E' => return Err(parse_error_response(&data).into()),
                _ => {}
            }
        }
    }

    /// Execute a simple query and return the result rows.
    ///
    /// Each element is a `Vec<Option<String>>` — one entry per column.
    /// Returns the error description string as `Err` on server error.
    async fn simple_query(&mut self, query: &str) -> Result<Vec<Vec<Option<String>>>, BoxError> {
        self.send_query(query).await?;
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        loop {
            let (msg_type, data) = self.read_message().await?;
            match msg_type {
                b'T' => {} // RowDescription — we use column position, not names
                b'D' => {
                    // DataRow
                    if data.len() < 2 {
                        continue;
                    }
                    let ncols = u16::from_be_bytes(data[0..2].try_into().unwrap()) as usize;
                    let mut row = Vec::with_capacity(ncols);
                    let mut pos = 2usize;
                    for _ in 0..ncols {
                        if pos + 4 > data.len() {
                            row.push(None);
                            continue;
                        }
                        let col_len = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
                        pos += 4;
                        if col_len < 0 {
                            row.push(None);
                        } else {
                            let end = pos + col_len as usize;
                            let s = std::str::from_utf8(&data[pos..end.min(data.len())])
                                .unwrap_or("")
                                .to_string();
                            pos = end;
                            row.push(Some(s));
                        }
                    }
                    rows.push(row);
                }
                b'C' => {}               // CommandComplete
                b'I' => {}               // EmptyQueryResponse
                b'Z' => return Ok(rows), // ReadyForQuery
                b'E' => {
                    let msg = parse_error_response(&data);
                    // Drain ReadyForQuery that follows ErrorResponse.
                    let _ = self.drain_until_ready().await;
                    return Err(msg.into());
                }
                _ => {}
            }
        }
    }

    /// Send `START_REPLICATION ...` and consume the `CopyBothResponse` ('W').
    ///
    /// After this call, `recv_copy_data` / `send_copy_data` are used to
    /// stream WAL messages and send keepalives.
    async fn start_replication(&mut self, query: &str) -> Result<(), BoxError> {
        self.send_query(query).await?;
        let (msg_type, data) = self.read_message().await?;
        match msg_type {
            b'W' => Ok(()), // CopyBothResponse
            b'E' => Err(parse_error_response(&data).into()),
            _ => Err(format!(
                "expected CopyBothResponse (W) for START_REPLICATION, got 0x{msg_type:02X}"
            )
            .into()),
        }
    }

    /// Receive one CopyData message from the walsender.
    ///
    /// Returns `Ok(Some(data))` for a `CopyData` ('d') message,
    /// `Ok(None)` for `CopyDone` ('c'), or `Err` on error / protocol violation.
    async fn recv_copy_data(&mut self) -> Result<Option<Bytes>, BoxError> {
        let (msg_type, data) = self.read_message().await?;
        match msg_type {
            b'd' => Ok(Some(Bytes::from(data))), // CopyData
            b'c' => Ok(None),                    // CopyDone
            b'E' => Err(parse_error_response(&data).into()),
            _ => Err(format!("unexpected message 0x{msg_type:02X} in CopyBoth stream").into()),
        }
    }

    /// Send a `CopyData` ('d') message to the walsender (e.g. StandbyStatusUpdate).
    async fn send_copy_data(&mut self, body: &[u8]) -> Result<(), BoxError> {
        // CopyData: [d][length:4 (includes itself)][body]
        let length = (4 + body.len()) as u32;
        let mut hdr = [0u8; 5];
        hdr[0] = b'd';
        hdr[1..5].copy_from_slice(&length.to_be_bytes());
        self.writer.write_all(&hdr).await?;
        self.writer.write_all(body).await?;
        self.writer.flush().await?;
        Ok(())
    }

    // ── Low-level I/O ─────────────────────────────────────────────────────────

    /// Send a simple-query `Query` message ('Q').
    async fn send_query(&mut self, query: &str) -> Result<(), BoxError> {
        // Query: [Q][length:4 (includes itself + null)][query\0]
        let body = query.as_bytes();
        let length = (4 + body.len() + 1) as u32;
        let mut hdr = [0u8; 5];
        hdr[0] = b'Q';
        hdr[1..5].copy_from_slice(&length.to_be_bytes());
        self.writer.write_all(&hdr).await?;
        self.writer.write_all(body).await?;
        self.writer.write_all(b"\0").await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Read one backend message: `[type(1)][length(4)][body(length-4)]`.
    async fn read_message(&mut self) -> Result<(u8, Vec<u8>), BoxError> {
        let mut header = [0u8; 5];
        self.reader.read_exact(&mut header).await?;
        let msg_type = header[0];
        let length = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        if length < 4 {
            return Err(format!(
                "protocol error: message 0x{msg_type:02X} has length {length} < 4"
            )
            .into());
        }
        let body_len = length - 4;
        let mut body = vec![0u8; body_len];
        self.reader.read_exact(&mut body).await?;
        Ok((msg_type, body))
    }
}

// ── Connection string helpers ─────────────────────────────────────────────────

/// Parse a libpq-style connstring into a key→value map.
///
/// Handles simple `key=value` pairs separated by whitespace.
/// Values containing spaces must be quoted (not yet supported — the default
/// connstring `host=/tmp port=5432 dbname=postgres` does not need quoting).
/// Parse a PostgreSQL LSN string (`"A/B"` hex) into a `u64`.
fn parse_lsn(s: &str) -> Result<u64, BoxError> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| format!("invalid LSN: {s}"))?;
    let hi = u64::from_str_radix(hi, 16).map_err(|_| format!("invalid LSN high: {hi}"))?;
    let lo = u64::from_str_radix(lo, 16).map_err(|_| format!("invalid LSN low: {lo}"))?;
    Ok((hi << 32) | lo)
}

fn parse_connstr(connstr: &str) -> HashMap<&str, &str> {
    let mut map = HashMap::new();
    for part in connstr.split_whitespace() {
        if let Some((k, v)) = part.split_once('=') {
            map.insert(k, v);
        }
    }
    map
}

/// Derive the Unix domain socket path from the connstring parameters.
///
/// PostgreSQL creates a socket at `{host}/.s.PGSQL.{port}`.
/// `host` defaults to `/tmp`, `port` defaults to `5432`.
fn unix_socket_path(params: &HashMap<&str, &str>) -> Result<String, BoxError> {
    let host = params.get("host").copied().unwrap_or("/tmp");
    let port = params.get("port").copied().unwrap_or("5432");
    // If host is an absolute path it is a socket directory.
    if host.starts_with('/') {
        Ok(format!("{host}/.s.PGSQL.{port}"))
    } else {
        Err(format!(
            "only Unix socket connections are supported (host must be an absolute path); got host={host}"
        )
        .into())
    }
}

/// Extract a human-readable error message from a PostgreSQL `ErrorResponse` ('E') body.
///
/// Each field in an ErrorResponse is: `[field_type(1)][value\0]`.
/// Field 'M' = message, 'C' = SQLSTATE code.
fn parse_error_response(data: &[u8]) -> String {
    let mut message = String::new();
    let mut sqlstate = String::new();
    let mut i = 0;
    while i < data.len() {
        let field_type = data[i];
        i += 1;
        if field_type == 0 {
            break;
        }
        // Find the null terminator for the field value.
        let start = i;
        while i < data.len() && data[i] != 0 {
            i += 1;
        }
        let value = std::str::from_utf8(&data[start..i]).unwrap_or("?");
        i += 1; // skip null
        match field_type {
            b'M' => message = value.to_string(),
            b'C' => sqlstate = value.to_string(),
            _ => {}
        }
    }
    if sqlstate.is_empty() {
        message
    } else {
        format!("{message} ({sqlstate})")
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::should_flush_tail;

    #[test]
    fn should_flush_tail_gate() {
        assert!(should_flush_tail(1, 2, true)); // generation advanced + tail present
        assert!(!should_flush_tail(1, 2, false)); // advanced but nothing buffered
        assert!(!should_flush_tail(2, 2, true)); // no checkpoint since last flush
        assert!(!should_flush_tail(2, 2, false)); // unchanged + nothing buffered
    }
}
