#!/usr/bin/env python3
"""vmtop — realtime VM swarm dashboard for tikovm-hostd.

Polls GET /vms every second and renders a color-coded TUI, with lifetime
counters (suspends/restores/proxy connections) scraped from GET /metrics.

Usage:
    ./vmtop.py [--api URL] [--interval SECS]
    ./vmtop.py --api http://10.0.0.1:9000 --interval 2

Default API: http://127.0.0.1:9000 (tikovm-hostd --api-listen default)
"""

import argparse
import curses
import json
import re
import time
import urllib.error
import urllib.request


# ── State → (curses color pair, indicator, label) ──────────────────────────
# VmState is serde snake_case (tikovm-protocol/src/vm.rs).

STATE_STYLE = {
    # stable, live
    "started":    (1, "●", "started"),
    "paused":     (2, "●", "paused"),
    # transitional
    "creating":   (3, "●", "creating"),
    "starting":   (3, "●", "starting"),
    "resuming":   (3, "●", "resuming"),
    "restoring":  (3, "●", "restoring"),
    "pausing":    (4, "●", "pausing"),
    "suspending": (4, "●", "suspending"),
    "destroying": (5, "●", "destroying"),
    # stable, not live
    "created":    (7, "○", "created"),
    "destroyed":  (5, "○", "destroyed"),
}

# Suspended: snapshot taken, VMM process gone (scale-to-zero)
SUSPENDED_STYLE = (6, "○", "suspended")


# ── API client ──────────────────────────────────────────────────────────────

def fetch_vms(api_url):
    """GET /vms — returns a bare JSON array of VmInfo dicts."""
    url = api_url.rstrip("/") + "/vms"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=3) as resp:
        data = json.loads(resp.read())
    return data if isinstance(data, list) else data.get("vms", [])


_COUNTER_RE = re.compile(r"^(tikovm_\w+)\s+(\d+(?:\.\d+)?)\s*$", re.M)


def fetch_counters(api_url):
    """GET /metrics (Prometheus text) — scrape the plain counters we show."""
    url = api_url.rstrip("/") + "/metrics"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=3) as resp:
        text = resp.read().decode("utf-8", "replace")
    want = (
        "tikovm_suspends_total",
        "tikovm_restores_total",
        "tikovm_proxy_connections_total",
    )
    out = {}
    for name, val in _COUNTER_RE.findall(text):
        if name in want:
            out[name] = int(float(val))
    return out


# ── Formatting helpers ──────────────────────────────────────────────────────

def truncate(s, width):
    if s is None:
        return "-"
    s = str(s)
    if len(s) <= width:
        return s
    return s[: width - 1] + "…"


# ── Column definitions ──────────────────────────────────────────────────────
# Tuple: (header, width, kind, extractor)
#   kind="text"   → extractor(vm) -> str
#   kind="state"  → special-cased colored indicator
#   kind="health" → special-cased colored ok/bad

COLUMNS = [
    ("VM ID",    20, "text",   lambda vm: truncate(vm.get("vm_id", "?"), 20)),
    ("STATE",    12, "state",  None),
    ("HEALTH",    7, "health", None),
    ("WORKLOAD", 12, "text",   lambda vm: truncate(vm.get("workload"), 12)),
    ("IP",       15, "text",   lambda vm: truncate(vm.get("guest_ip"), 15)),
]


# ── Rendering ───────────────────────────────────────────────────────────────

def get_state_style(vm):
    state = vm.get("state")
    if state == "suspended":
        return SUSPENDED_STYLE
    if state and state in STATE_STYLE:
        return STATE_STYLE[state]
    return (5, "?", state or "unknown")


def render(stdscr, vms, counters, error_msg, api_url):
    stdscr.erase()
    h, w = stdscr.getmaxyx()

    # ── Header ──
    live = sum(1 for v in vms if v.get("state") not in ("suspended", "created", "destroyed"))
    suspended = sum(1 for v in vms if v.get("state") == "suspended")
    ts = time.strftime("%H:%M:%S", time.localtime())
    header = f"  tikovm swarm   hostd: {api_url}  VMs: {len(vms)} ({live} live, {suspended} suspended)"
    if counters:
        header += (
            f"  suspends: {counters.get('tikovm_suspends_total', 0)}"
            f"  restores: {counters.get('tikovm_restores_total', 0)}"
            f"  proxy_conns: {counters.get('tikovm_proxy_connections_total', 0)}"
        )
    header += f"  {ts}"
    if len(header) > w:
        header = header[: w - 1]
    stdscr.attron(curses.color_pair(3) | curses.A_BOLD)
    stdscr.addstr(0, 0, header.ljust(w)[:w])
    stdscr.attroff(curses.color_pair(3) | curses.A_BOLD)

    # ── Column header row ──
    col_x = []
    x = 2  # left margin
    row_y = 2
    header_parts = []
    for name, width, _, _ in COLUMNS:
        col_x.append((x, width))
        header_parts.append(name.ljust(width)[:width])
        x += width + 1
    col_header = " ".join(header_parts)
    stdscr.attron(curses.A_DIM)
    stdscr.addstr(row_y, 1, col_header[: w - 1])
    stdscr.attroff(curses.A_DIM)

    # Separator line
    sep_y = row_y + 1
    stdscr.attron(curses.A_DIM)
    stdscr.addstr(sep_y, 1, "─" * min(w - 2, len(col_header) + 2))
    stdscr.attroff(curses.A_DIM)

    # ── VM rows ──
    y = sep_y + 1
    for vm in vms:
        if y >= h - 1:
            break
        pair, indicator, label = get_state_style(vm)

        for idx, (_, cw, kind, extractor) in enumerate(COLUMNS):
            cx = col_x[idx][0]
            if cx + cw > w:
                break

            if kind == "state":
                attr = curses.color_pair(pair) | curses.A_BOLD
                val = f"{indicator} {label}"
                stdscr.attron(attr)
                stdscr.addstr(y, cx, val.ljust(cw)[:cw])
                stdscr.attroff(attr)
            elif kind == "health":
                healthy = vm.get("healthy")
                if healthy is None:
                    stdscr.attron(curses.A_DIM)
                    stdscr.addstr(y, cx, "-".ljust(cw)[:cw])
                    stdscr.attroff(curses.A_DIM)
                elif healthy:
                    stdscr.attron(curses.color_pair(1) | curses.A_BOLD)
                    stdscr.addstr(y, cx, "● ok".ljust(cw)[:cw])
                    stdscr.attroff(curses.color_pair(1) | curses.A_BOLD)
                else:
                    stdscr.attron(curses.color_pair(5) | curses.A_BOLD)
                    stdscr.addstr(y, cx, "● bad".ljust(cw)[:cw])
                    stdscr.attroff(curses.color_pair(5) | curses.A_BOLD)
            else:
                val = extractor(vm)
                stdscr.addstr(y, cx, str(val).ljust(cw)[:cw])
        y += 1

    # ── Footer ──
    footer_y = h - 1
    if error_msg:
        stdscr.attron(curses.color_pair(5) | curses.A_BOLD)
        err_text = f" ⚠ {error_msg}"
        stdscr.addstr(footer_y, 0, err_text.ljust(w)[:w])
        stdscr.attroff(curses.color_pair(5) | curses.A_BOLD)
    else:
        stdscr.attron(curses.A_DIM)
        stdscr.addstr(footer_y, 0, " q to exit".ljust(w)[:w])
        stdscr.attroff(curses.A_DIM)

    stdscr.refresh()


def main_loop(stdscr, args):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(int(args.interval * 1000))

    # Initialize color pairs
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_GREEN, -1)    # started / healthy
    curses.init_pair(2, curses.COLOR_YELLOW, -1)   # paused
    curses.init_pair(3, curses.COLOR_CYAN, -1)     # starting/restoring/…
    curses.init_pair(4, curses.COLOR_MAGENTA, -1)  # pausing/suspending
    curses.init_pair(5, curses.COLOR_RED, -1)      # destroyed/error / unhealthy
    curses.init_pair(6, curses.COLOR_BLUE, -1)     # suspended (scale-to-zero)
    curses.init_pair(7, curses.COLOR_WHITE, -1)    # created

    api_url = args.api
    error_msg = None
    last_vms = []
    last_counters = {}

    while True:
        # ── Fetch ──
        try:
            vms = fetch_vms(api_url)
            # Live VMs first, then suspended, then dead; by vm_id within group.
            order = {"suspended": 1, "created": 2, "destroyed": 3}
            vms.sort(key=lambda v: (order.get(v.get("state"), 0), v.get("vm_id", "")))
            last_vms = vms
            try:
                last_counters = fetch_counters(api_url)
            except Exception:
                pass  # /metrics is best-effort; keep last good values
            error_msg = None
        except (urllib.error.URLError, urllib.error.HTTPError, OSError, json.JSONDecodeError) as e:
            error_msg = f"{type(e).__name__}: {e}"
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"

        # ── Render ──
        try:
            render(stdscr, last_vms, last_counters, error_msg, api_url)
        except curses.error:
            pass

        # ── Wait (interruptible) ──
        try:
            ch = stdscr.getch()
            if ch == ord("q") or ch == ord("Q"):
                break
        except curses.error:
            pass


def main():
    parser = argparse.ArgumentParser(description="tikovm VM swarm dashboard")
    parser.add_argument("--api", default="http://127.0.0.1:9000",
                        help="tikovm-hostd API URL (default: http://127.0.0.1:9000)")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="refresh interval in seconds (default: 1.0)")
    args = parser.parse_args()

    try:
        curses.wrapper(main_loop, args)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
