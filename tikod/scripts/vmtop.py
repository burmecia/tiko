#!/usr/bin/env python3
"""vmtop — realtime VM swarm dashboard for tikod.

Polls GET /vms every second and renders a color-coded TUI.

Usage:
    ./vmtop.py [--api URL] [--interval SECS]
    ./vmtop.py --api http://10.0.0.1:9000 --interval 2

Default API: http://127.0.0.1:9000
"""

import argparse
import curses
import json
import time
import urllib.error
import urllib.request


# ── State → (curses color pair, indicator, label) ──────────────────────────

STATE_STYLE = {
    "running":      (1, "●", "running"),
    "paused":       (2, "●", "paused"),
    "starting":     (3, "●", "starting"),
    "restoring":    (3, "●", "restoring"),
    "snapshotting": (4, "●", "snapshotting"),
    "stopped":      (5, "●", "stopped"),
}

# Scaled-to-zero: no live state but registered with a snapshot
SCALED_ZERO_STYLE = (6, "○", "scaled-0")


# ── API client ──────────────────────────────────────────────────────────────

def fetch_vms(api_url):
    """GET /vms and return list of vm dicts, or raise."""
    url = api_url.rstrip("/") + "/vms"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=3) as resp:
        data = json.loads(resp.read())
    return data.get("vms", [])


# ── Formatting helpers ──────────────────────────────────────────────────────

def fmt_bytes(n):
    if n is None:
        return "-"
    n = float(n)
    for unit in ("B", "K", "M", "G", "T"):
        if abs(n) < 1024:
            return f"{n:.0f}{unit}" if unit in ("B",) else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}P"


def fmt_secs(n):
    if n is None:
        return "-"
    if n < 60:
        return f"{n}s"
    if n < 3600:
        return f"{n // 60}m"
    return f"{n // 3600}h"


def truncate(s, width):
    if s is None:
        return "-"
    s = str(s)
    if len(s) <= width:
        return s
    return s[: width - 1] + "…"


# ── Column definitions ──────────────────────────────────────────────────────
# Tuple: (header, width, kind)
#   kind="text"  → extractor(vm, metrics) -> str
#   kind="state" → special-cased colored indicator
#   kind="db"    → special-cased colored up/down


def get_conns(vm, m):
    c = vm.get("connection_count")
    if c:
        return str(c)
    if m and m.get("connections") is not None:
        return str(m["connections"])
    return "0"


COLUMNS = [
    ("VM ID",   18, "text",  lambda vm, m: truncate(vm.get("vm_id", "?"), 18)),
    ("STATE",   12, "state", None),
    ("DB",       6, "db",    None),
    ("IP",      15, "text",  lambda vm, m: truncate(vm.get("guest_ip"), 15)),
    ("CONNS",    5, "text",  get_conns),
    ("EPOCH",    5, "text",  lambda vm, m: str(vm.get("pause_epoch")) if vm.get("pause_epoch") is not None else "-"),
    ("CACHE%",   7, "text",  lambda vm, m: f"{m['cache_hit_ratio']:.1%}" if m and m.get("cache_hit_ratio") is not None else "-"),
    ("DB SIZE",  8, "text",  lambda vm, m: fmt_bytes(m.get("db_size_bytes")) if m else "-"),
    ("WAL LSN", 12, "text",  lambda vm, m: truncate(m.get("wal_lsn"), 12) if m else "-"),
    ("AGE",      6, "text",  lambda vm, m: fmt_secs(vm.get("last_report_secs_ago"))),
]


# ── Rendering ───────────────────────────────────────────────────────────────

def get_state_style(vm):
    state = vm.get("state")
    if state and state in STATE_STYLE:
        return STATE_STYLE[state]
    # No live state but registered → scaled to zero
    if vm.get("snapshot_id") or vm.get("tenant_id"):
        return SCALED_ZERO_STYLE
    return (5, "?", "unknown")


def render(stdscr, vms, error_msg, api_url, total_count):
    stdscr.erase()
    h, w = stdscr.getmaxyx()

    # ── Header ──
    title = " tiko swarm "
    ts = time.strftime("%H:%M:%S", time.localtime())
    header = f"  {title}  tikod: {api_url}  VMs: {total_count}  {ts}"
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
        m = vm.get("last_metrics") or {}
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
            elif kind == "db":
                if not m:
                    stdscr.attron(curses.A_DIM)
                    stdscr.addstr(y, cx, "-".ljust(cw)[:cw])
                    stdscr.attroff(curses.A_DIM)
                elif m.get("available"):
                    stdscr.attron(curses.color_pair(1) | curses.A_BOLD)
                    stdscr.addstr(y, cx, "● up".ljust(cw)[:cw])
                    stdscr.attroff(curses.color_pair(1) | curses.A_BOLD)
                else:
                    stdscr.attron(curses.color_pair(5) | curses.A_BOLD)
                    stdscr.addstr(y, cx, "● down".ljust(cw)[:cw])
                    stdscr.attroff(curses.color_pair(5) | curses.A_BOLD)
            else:
                val = extractor(vm, m)
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
        stdscr.addstr(footer_y, 0, " Ctrl-C to exit".ljust(w)[:w])
        stdscr.attroff(curses.A_DIM)

    stdscr.refresh()


def main_loop(stdscr, args):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(int(args.interval * 1000))

    # Initialize color pairs
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_GREEN, -1)    # running / db up
    curses.init_pair(2, curses.COLOR_YELLOW, -1)   # paused
    curses.init_pair(3, curses.COLOR_CYAN, -1)     # starting/restoring
    curses.init_pair(4, curses.COLOR_MAGENTA, -1)  # snapshotting
    curses.init_pair(5, curses.COLOR_RED, -1)      # stopped/error / db down
    curses.init_pair(6, curses.COLOR_BLUE, -1)     # scaled to zero

    api_url = args.api
    error_msg = None
    last_vms = []

    while True:
        # ── Fetch ──
        try:
            vms = fetch_vms(api_url)
            vms.sort(key=lambda v: v.get("vm_id", ""))
            error_msg = None
            last_vms = vms
        except (urllib.error.URLError, urllib.error.HTTPError, OSError, json.JSONDecodeError) as e:
            error_msg = f"{type(e).__name__}: {e}"
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"

        # ── Render ──
        try:
            render(stdscr, last_vms, error_msg, api_url, len(last_vms))
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
    parser = argparse.ArgumentParser(description="tikod VM swarm dashboard")
    parser.add_argument("--api", default="http://127.0.0.1:9000",
                        help="tikod API URL (default: http://127.0.0.1:9000)")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="refresh interval in seconds (default: 1.0)")
    args = parser.parse_args()

    try:
        curses.wrapper(main_loop, args)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
