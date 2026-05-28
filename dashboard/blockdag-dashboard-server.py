#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BlockDAG Dashboard Server
Run:  python blockdag-dashboard-server.py
Open: http://localhost:8088
"""

import sys, io
# Force UTF-8 stdout/stderr so non-ASCII in print() doesn't crash on Windows cp1252 consoles
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)

import json, os, shutil, subprocess, urllib.request, re, threading, time, base64, webbrowser
try:
    import winreg as _winreg   # Windows only — used for HKCU Run autostart
except ImportError:
    _winreg = None
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# ── Suppress CMD flash + taskbar icon on Windows ──────────────────────────────
# subprocess.run/Popen without flags briefly flashes a CMD window AND shows a
# taskbar button for each docker call.  Two layers are needed:
#   CREATE_NO_WINDOW  — tells Windows not to allocate a console for the process
#   STARTUPINFO/SW_HIDE — tells the process not to show its initial window
# Calls that already set creationflags (e.g. _restart_server DETACHED_PROCESS)
# are left untouched because setdefault only writes missing keys.
# The shell=True log-popup Popen uses `start` which opens its own visible
# window independently — not affected by these flags on the parent cmd.exe.
_NO_WIN = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
if _NO_WIN:
    def _si():
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = 0  # SW_HIDE
        return si
    _orig_run, _orig_popen = subprocess.run, subprocess.Popen
    def _run_no_win(*a, **kw):
        kw.setdefault('creationflags', _NO_WIN)
        kw.setdefault('startupinfo', _si())
        return _orig_run(*a, **kw)
    def _popen_no_win(*a, **kw):
        kw.setdefault('creationflags', _NO_WIN)
        kw.setdefault('startupinfo', _si())
        return _orig_popen(*a, **kw)
    subprocess.run, subprocess.Popen = _run_no_win, _popen_no_win

# ── Configuration ─────────────────────────────────────────────────────────────
PORT          = 8088
RPC_URL       = "http://localhost:38131"
INSTALL_DIR   = Path(r"C:\blockdag node")
NODE1_DATA    = str(INSTALL_DIR / "chain-data" / "node1")
NODE2_DATA    = str(INSTALL_DIR / "chain-data" / "node2")
BACKUP_DIR    = str(INSTALL_DIR / "data-restore" / "backups")
ENV_FILE      = INSTALL_DIR / ".env"
ENV_POOL_FILE = INSTALL_DIR / "asic-pool" / ".env"
# ──────────────────────────────────────────────────────────────────────────────

# Keys exposed to the dashboard config modal
ENV_EXPOSED_KEYS = [
    "MINING_ADDRESS", "NODE_RPC_USER", "NODE_RPC_PASS",
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "PG_URL",
    "POOL_FEE_PERCENTAGE", "POOL_STARTING_PDIFF", "POOL_PORT", "BDAG_MINER_POOL_PASSWORD",
    "NODE_CACHE_MB", "NODE_DAG_CACHE", "NODE_BD_CACHE", "NODE_MAX_PEERS",
]

CONTAINERS  = ["bdag-miner-node-1", "bdag-miner-node-2",
               "asic-pool", "rpc-failover", "pool-db"]
DASH_FILE        = Path(__file__).parent / "blockdag-dashboard.html"
ALERTS_FILE      = Path(__file__).parent / "alerts.json"
PAYOUT_FILE      = Path(__file__).parent / "payout.json"
BACKUP_CFG_FILE  = Path(__file__).parent / "backup-config.json"
ALERT_HISTORY_FILE = Path(__file__).parent / "alert-history.json"
MAINTENANCE_FILE  = Path(__file__).parent / "maintenance.json"
WATCHDOG_CFG_FILE   = Path(__file__).parent / "watchdog-config.json"
AUTOSTART_REG_KEY  = r"Software\Microsoft\Windows\CurrentVersion\Run"
AUTOSTART_REG_NAME = "BlockDAG-AutoStart"
PEERS_FILE         = Path(__file__).parent / "Network Peers.txt"
PEERS_MANAGED_FILE = Path(__file__).parent / "peers-managed.txt"
COMPOSE_FILE       = INSTALL_DIR / "docker-compose.yml"
ENV_FILE           = INSTALL_DIR / ".env"
MAX_ALERT_HISTORY  = 50
ALERT_COOLDOWN = 900  # seconds between repeat alerts for same condition

def _get_backup_dir():
    """Return the active backup directory — default or user-configured."""
    try:
        if BACKUP_CFG_FILE.exists():
            cfg = json.loads(BACKUP_CFG_FILE.read_text())
            custom = cfg.get("backup_dir", "").strip()
            if custom:
                return custom
    except Exception:
        pass
    return BACKUP_DIR

_alert_lock  = threading.Lock()
_alert_state = {
    "freeze":     {"active": False, "last_sent": 0.0},
    "peers":      {"active": False, "last_sent": 0.0},
    "divergence": {"active": False, "last_sent": 0.0},
    "disk":       {"active": False, "last_sent": 0.0},
}
_last_auto_add_time = None   # ISO string, updated when auto-add fires

# ── Node freeze watchdog ──────────────────────────────────────────────────────
WATCHDOG_INTERVAL   = 120   # seconds between checks
WATCHDOG_GRACE      = 300   # seconds of confirmed freeze before auto-restart
WATCHDOG_STARTUP    = 300   # ignore containers that restarted < 5 min ago
WATCHDOG_CONTAINERS = ["bdag-miner-node-1", "bdag-miner-node-2"]

_watchdog_lock  = threading.Lock()
_watchdog_state = {}        # container -> {"frozen_since": float|None}

# ── Pool nonce watchdog ───────────────────────────────────────────────────────
POOL_WATCHDOG_INTERVAL = 60    # seconds between checks
POOL_WATCHDOG_GRACE    = 180   # seconds of continuous nonce errors before restart
POOL_WATCHDOG_STARTUP  = 120   # ignore if pool restarted < 2 min ago

_pool_watchdog_lock  = threading.Lock()
_pool_watchdog_state = {"nonce_error_since": None}

_node_id_cache      = {}     # container_name -> node_id string
_node_version_cache = {}     # container_name -> version string (cached from startup log)

# ---------------------------------------------------------------------------
# secp256k1 peer-ID derivation (pure Python stdlib — no external deps)
# Derives the libp2p peer ID from a 64-char hex private key (network.key).
# ---------------------------------------------------------------------------
_SEC_P  = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
_SEC_GX = 0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798
_SEC_GY = 0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8
_B58_AL = b'123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

def _sec_pt_add(P, Q):
    p = _SEC_P
    if P is None: return Q
    if Q is None: return P
    if P[0] == Q[0]:
        if P[1] != Q[1]: return None
        lam = (3 * P[0] * P[0]) * pow(2 * P[1], p - 2, p) % p
    else:
        lam = (Q[1] - P[1]) * pow(Q[0] - P[0], p - 2, p) % p
    x = (lam * lam - P[0] - Q[0]) % p
    y = (lam * (P[0] - x) - P[1]) % p
    return (x, y)

def _sec_scalar_mult(k, P):
    R, Q = None, P
    while k:
        if k & 1:
            R = _sec_pt_add(R, Q)
        Q = _sec_pt_add(Q, Q)
        k >>= 1
    return R

def _b58enc(data: bytes) -> str:
    n = int.from_bytes(data, 'big')
    res = []
    while n:
        n, r = divmod(n, 58)
        res.append(_B58_AL[r:r+1])
    res.reverse()
    # Preserve leading zero bytes as '1' characters
    pad = 0
    for b in data:
        if b == 0: pad += 1
        else: break
    return (_B58_AL[0:1] * pad + b''.join(res)).decode()

def _derive_peer_id(hex_key: str) -> str | None:
    """Derive libp2p peer ID from a hex-encoded secp256k1 private key."""
    try:
        hex_key = hex_key.strip()
        if len(hex_key) != 64:
            return None
        k = int(hex_key, 16)
        G = (_SEC_GX, _SEC_GY)
        pt = _sec_scalar_mult(k, G)
        if pt is None:
            return None
        # Compressed public key (33 bytes)
        prefix = b'\x02' if pt[1] % 2 == 0 else b'\x03'
        pub = prefix + pt[0].to_bytes(32, 'big')
        # Protobuf PublicKey: field1=key_type(secp256k1=2), field2=key_data(33 bytes)
        proto = b'\x08\x02\x12\x21' + pub   # 4 + 33 = 37 bytes
        # Identity multihash: code=0x00, length=0x25 (37), then the key bytes
        mh = b'\x00\x25' + proto            # 39 bytes total
        return _b58enc(mh)
    except Exception:
        return None

# DagTech miner config.env search paths (wallet → worker_name lookup)
_DAGTECH_CONFIG_PATHS = [
    Path(os.environ.get("USERPROFILE", "C:/Users/User")) / ".dagtech-miner" / "config.env",
    Path("C:/dagtech-miner/config.env"),
]

def _dagtech_worker_map():
    """Read all DagTech config.env files and return {wallet_lower: worker_name}."""
    mapping = {}
    for cfg in _DAGTECH_CONFIG_PATHS:
        try:
            for line in cfg.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip().upper(), v.strip()
                if k == "WALLET":
                    mapping["_wallet"] = v.lower()
                elif k == "WORKER_NAME":
                    mapping["_worker"] = v
            w = mapping.pop("_wallet", None)
            n = mapping.pop("_worker", None)
            if w and n:
                mapping[w] = n
        except Exception:
            pass
    return mapping


# ── Remote miner polling (BDAG_MINER_SCAN_TARGET) ───────────────────────────
def _parse_remote_miners():
    """
    Poll remote DagTech miner control servers listed in BDAG_MINER_SCAN_TARGET
    (comma-separated IPs or IP:port, default port 8880).  Each reachable miner
    contributes one entry to the workers table showing its live hashrate, wallet,
    worker name, and share counters — independent of pool-log tracking.
    """
    # Read from .env file (not os.environ — the server doesn't load .env into env)
    targets = (_env_read().get("BDAG_MINER_SCAN_TARGET") or
               os.environ.get("BDAG_MINER_SCAN_TARGET") or "").strip()
    if not targets:
        return []
    results = []
    for raw in targets.split(","):
        raw = raw.strip()
        if not raw:
            continue
        if ":" in raw and not raw.startswith("["):   # host:port (not IPv6)
            host, _, port = raw.rpartition(":")
        else:
            host, port = raw, "8880"
        try:
            req = urllib.request.Request(
                f"http://{host}:{port}/metrics",
                headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=3) as r:
                data = json.loads(r.read())
            wallet = (data.get("wallet") or "").strip()
            if not wallet:
                continue
            hr_hs = float(data.get("hashrate") or 0)   # H/s from control server
            results.append({
                "address":      wallet,
                "worker":       (data.get("worker") or data.get("worker_name") or "").strip(),
                "ip":           host,
                "difficulty":   float(data.get("difficulty") or 0),
                "last_active":  "live",
                "accepted":     int(data.get("accepted") or 0),
                "rejected":     int(data.get("rejected") or 0),
                "hashrate_mhs": round(hr_hs / 1e6, 6),
                "blocks_found": 0,
            })
        except Exception:
            pass
    return results


# ── Pool log regex patterns (for per-worker extraction) ──────────────────────
_POOL_LOG_TS_RE    = re.compile(r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})")
_POOL_AUTH_RE      = re.compile(r"\[((?:\d{1,3}\.){3}\d{1,3}):([0-9]+)\]\s+authorize accepted user=([^\s]+)")
_POOL_PUSHDIF_RE   = re.compile(r"PUSHDIF\s+->\s+((?:\d{1,3}\.){3}\d{1,3}):([0-9]+)\s+mining\.set_difficulty\s+([0-9.]+)")
_POOL_SHARE_RE          = re.compile(r"valid share accepted\s+([0-9.]+)\s+[^0-9]+[0-9]+\s+worker=([^\s]+)")
_POOL_SUPPRESSED_RE     = re.compile(r"suppressedDiff=([0-9.]+)")
_POOL_SUPPRESSED_CNT_RE = re.compile(r"\bsuppressed=(\d+)\b")
_POOL_SUBMIT_RE  = re.compile(r"submit from worker=([^\s]+)")
_POOL_ANSI_RE    = re.compile(r"\x1b\[[0-9;]*m")
_POOL_VARDIFF_RE = re.compile(
    r"\[vardiff DEBUG\]\s+(?:hold|increase|decrease) pdiff ([0-9.]+) \(shares=(\d+) in (\d+)s"
)


def _parse_pool_workers():
    """
    Parse the asic-pool container logs to build a per-port worker list.

    Each TCP connection (unique port) is one row, regardless of how many
    ports share the same wallet address.  Since the pool only logs
    'worker=<wallet>' on share events (never the port), per-port hashrate is
    estimated using each port's current vardiff difficulty as a proxy.

    Why difficulty = hashrate proxy
    --------------------------------
    The pool's vardiff adjusts each connection's difficulty to maintain a
    constant share rate (~0.33 shares/sec).  A miner doing 5× the work gets
    5× the difficulty.  So: hashrate_i ∝ difficulty_i.

    Formula (preferred — VARDIFF direct measurement)
    -------------------------------------------------
    The pool logs per-connection share performance every 60 s:
      [vardiff DEBUG] hold pdiff X (shares=N in Ys, ratio=R, ...)
    These directly measure how fast each connection is finding shares.
      share_rate_per_port = sum(shares) / sum(period)  [from VARDIFF lines]
      hashrate_per_port   = difficulty_i × share_rate_per_port

    Ports are matched by their pdiff value (set by PUSHDIF; the same float
    the pool tracks internally for vardiff).

    Fallback formula (when no VARDIFF data for a port)
    ---------------------------------------------------
    If no VARDIFF line exists for a given port's pdiff value, fall back to
    the share-count estimate:
      actual_shares_10m  = sum(1 + suppressed_count) for all logged events
                           in the 10-min window
      share_rate_per_port = actual_shares_10m / n_active_ports / 600s

    Why NOT use (acceptedDiff + suppressedDiff) / time
    ---------------------------------------------------
    suppressedDiff is the sum of batched shares at varying difficulties
    during vardiff adjustment periods.  Because difficulty is rising, the
    suppressed shares accumulate at higher-and-higher difficulty values,
    inflating the apparent total by ~10× vs the stable PUSHDIF difficulty.

    Three-pass strategy
    -------------------
    Pass 0 (pre-pass) — find log_now (latest timestamp) so ten_min_ago and
      five_min_ago are fixed anchors, not a moving target.
    Pass 1 — main scan:
      AUTH_ACCEPT  → register (ip, port) entry
      PUSHDIF      → update difficulty + last-seen; seeds placeholders for
                     ports whose auth event scrolled past the --tail window
      valid share  → count actual shares (1 + suppressed_count) in 10-min
                     window; count accepted-event total for display
      vardiff DEBUG → accumulate (shares, period) per pdiff value in 10-min
                      window for direct share-rate measurement
    Pass 2 — compute per-port hashrate and filter to active ports:
      active ports  = last PUSHDIF within 5 min of log_now
    """
    try:
        r = subprocess.run(
            ["docker", "logs", "--tail", "5000", "asic-pool"],
            capture_output=True, text=True, timeout=15)
        lines = [_POOL_ANSI_RE.sub("", l) for l in (r.stdout + r.stderr).splitlines()]
    except Exception:
        return []

    workers             = {}   # (ip, port) → worker dict
    wallet_shares_10m   = {}   # wallet_str → actual share count in 10-min window (incl. suppressed)
    wallet_shares_total = {}   # wallet_str → total accepted-event count (all time, for display)
    vardiff_by_pdiff    = {}   # pdiff_float → [shares_sum, period_sum] from vardiff DEBUG lines

    # ── Pre-pass: find the latest timestamp in the log ────────────────────────
    # Must be done before the main scan so ten_min_ago/five_min_ago are fixed
    # anchor points, not a moving target that causes every share to qualify.
    log_now = None
    for line in lines:
        ts_m = _POOL_LOG_TS_RE.match(line)
        if ts_m:
            try:
                ts_dt = datetime.strptime(ts_m.group(1), "%Y/%m/%d %H:%M:%S")
                if log_now is None or ts_dt > log_now:
                    log_now = ts_dt
            except Exception:
                pass
    if log_now is None:
        log_now = datetime.now()

    ten_min_ago  = log_now - timedelta(minutes=10)
    five_min_ago = log_now - timedelta(minutes=5)

    # ── Main scan ─────────────────────────────────────────────────────────────
    for line in lines:
        ts_str = ""
        ts_dt  = None
        ts_m = _POOL_LOG_TS_RE.match(line)
        if ts_m:
            raw    = ts_m.group(1)
            ts_str = raw[:10].replace("/", "-") + raw[10:16]
            try:
                ts_dt = datetime.strptime(raw, "%Y/%m/%d %H:%M:%S")
            except Exception:
                pass

        auth = _POOL_AUTH_RE.search(line)
        if auth:
            ip, port, user = auth.group(1), auth.group(2), auth.group(3)
            dot    = user.rfind(".")
            wallet = user[:dot] if dot > 0 else user
            wname  = user[dot + 1:] if dot > 0 else ""
            key    = (ip, port)
            if key not in workers:
                workers[key] = {
                    "address":          wallet,
                    "worker":           wname,
                    "ip":               ip,
                    "port":             port,
                    "difficulty":       0,
                    "last_active":      ts_str,
                    "accepted":         0,
                    "rejected":         0,
                    "hashrate_mhs":     0,
                    "_wallet":          wallet,
                    "_last_pushdif_dt": None,
                }
            else:
                workers[key]["address"] = wallet
                workers[key]["worker"]  = wname
                workers[key]["_wallet"] = wallet
                if ts_str > workers[key]["last_active"]:
                    workers[key]["last_active"] = ts_str
            continue

        diff_m = _POOL_PUSHDIF_RE.search(line)
        if diff_m:
            ip, port = diff_m.group(1), diff_m.group(2)
            key = (ip, port)
            if key not in workers:
                # Auth scrolled out of window — placeholder so this port still shows
                workers[key] = {
                    "address":          "",
                    "worker":           "",
                    "ip":               ip,
                    "port":             port,
                    "difficulty":       float(diff_m.group(3)),
                    "last_active":      ts_str,
                    "accepted":         0,
                    "rejected":         0,
                    "hashrate_mhs":     0,
                    "_wallet":          "",
                    "_last_pushdif_dt": ts_dt,
                }
            else:
                workers[key]["difficulty"]       = float(diff_m.group(3))
                workers[key]["_last_pushdif_dt"] = ts_dt
                if ts_str > workers[key].get("last_active", ""):
                    workers[key]["last_active"] = ts_str
            continue

        share = _POOL_SHARE_RE.search(line)
        if share:
            wallet = share.group(2)   # always the bare wallet address
            # Total accepted events (all time) — shown in the Shares column
            wallet_shares_total[wallet] = wallet_shares_total.get(wallet, 0) + 1
            # Actual share count including suppressed — used for hashrate fallback
            if ts_dt and ts_dt >= ten_min_ago:
                cnt_m  = _POOL_SUPPRESSED_CNT_RE.search(line)
                actual = (int(cnt_m.group(1)) + 1) if cnt_m else 1
                wallet_shares_10m[wallet] = wallet_shares_10m.get(wallet, 0) + actual
            continue

        vd_m = _POOL_VARDIFF_RE.search(line)
        if vd_m and ts_dt and ts_dt >= ten_min_ago:
            # Accumulate per-pdiff share measurements for direct hashrate calculation.
            # The pool emits these every 60 s per connection; pdiff matches the
            # difficulty sent via PUSHDIF so we can map to a specific port.
            pdiff  = float(vd_m.group(1))
            shares = int(vd_m.group(2))
            period = int(vd_m.group(3))
            if pdiff not in vardiff_by_pdiff:
                vardiff_by_pdiff[pdiff] = [0, 0]
            vardiff_by_pdiff[pdiff][0] += shares
            vardiff_by_pdiff[pdiff][1] += period

    # ── Pass 2: compute per-port hashrate and filter to active ports ──────────
    # Only ports with a PUSHDIF in the last 5 min of log time are "active".
    # Hashrate per port = difficulty × share_rate_per_port.
    # All ports targeting the same share rate (set by vardiff), so difficulty
    # is directly proportional to each miner's actual hash rate.
    active_by_wallet = {}   # wallet → [worker_dict, ...]
    for w in workers.values():
        last_pd = w.get("_last_pushdif_dt")
        if last_pd and last_pd >= five_min_ago:
            wlt = w["_wallet"]
            active_by_wallet.setdefault(wlt, []).append(w)

    result = []
    for wallet, ports in active_by_wallet.items():
        n             = len(ports)
        actual_10m    = wallet_shares_10m.get(wallet, 0)
        total_acc     = wallet_shares_total.get(wallet, 0)
        # Fallback share rate: actual-share-count method evenly split across ports
        fallback_rate = actual_10m / n / 600 if actual_10m > 0 else 0
        per_port_acc  = total_acc // n if total_acc > 0 else 0
        for w in ports:
            pdiff = w["difficulty"]
            vd    = vardiff_by_pdiff.get(pdiff)
            if vd and vd[1] > 0:
                # Preferred: direct VARDIFF measurement — shares/sec for this connection
                share_rate = vd[0] / vd[1]
            else:
                # Fallback: derived from accepted-share counts, split evenly across ports
                share_rate = fallback_rate
            # hashrate (MH/s) = stratum_diff × 65536 (hashes/unit) × share_rate / 1e6
            # 65536 = 2^16, the per-difficulty-unit hash count for this coin's target
            w["hashrate_mhs"] = round(pdiff * 65536 * share_rate / 1e6, 4)
            w["accepted"]     = per_port_acc
            w.pop("_wallet", None)
            w.pop("_last_pushdif_dt", None)
            result.append(w)

    return sorted(result, key=lambda x: x.get("last_active") or "", reverse=True)


def _parse_block_finders(block_times):
    """
    Match pool log 'submit from worker=wallet.workername' events to blocks.

    block_times: list of (hash_str, created_at_datetime)
    Returns:     dict {hash_str: "wallet.workername"}

    Strategy: for each block, find the submit log line whose timestamp falls
    within 90 seconds before the block's DB created_at.  We use --tail 20000
    so multi-day history is covered (blocks are rare events).
    """
    if not block_times:
        return {}
    try:
        r = subprocess.run(
            ["docker", "logs", "--tail", "20000", "asic-pool"],
            capture_output=True, text=True, timeout=15)
        lines = [_POOL_ANSI_RE.sub("", l) for l in (r.stdout + r.stderr).splitlines()]
    except Exception:
        return {}

    # Collect all submit events as (datetime, worker_str)
    submit_events = []
    for line in lines:
        ts_m  = _POOL_LOG_TS_RE.match(line)
        sub_m = _POOL_SUBMIT_RE.search(line)
        if ts_m and sub_m:
            try:
                ts_dt = datetime.strptime(ts_m.group(1), "%Y/%m/%d %H:%M:%S")
                submit_events.append((ts_dt, sub_m.group(1)))
            except Exception:
                pass

    finders = {}
    try:
        for block_hash, block_dt in block_times:
            if not block_dt or not isinstance(block_dt, datetime):
                continue
            # Strip timezone info for naive comparison (pool logs are local time)
            if hasattr(block_dt, "tzinfo") and block_dt.tzinfo is not None:
                block_dt = block_dt.replace(tzinfo=None)
            best_worker = None
            best_delta  = None
            for sub_dt, worker in submit_events:
                try:
                    delta = (block_dt - sub_dt).total_seconds()
                except Exception:
                    continue
                # Accept submits 0–90 s before the block was recorded in DB
                if 0 <= delta <= 90:
                    if best_delta is None or delta < best_delta:
                        best_delta = delta
                        best_worker = worker
            if best_worker:
                finders[block_hash] = best_worker
    except Exception:
        pass
    return finders


# ── Per-worker block count cache ──────────────────────────────────────────────
_blocks_per_worker_cache: dict = {"data": {}, "ts": 0.0}
_BLOCKS_WORKER_CACHE_SECS = 300   # refresh every 5 minutes


def _blocks_per_worker_from_logs() -> dict:
    """
    Return {worker_string: block_count} by matching pool-log submit events to
    every block in the DB.  Worker string is 'wallet.workername' or bare wallet.

    Results are cached for 5 minutes so the expensive log + DB scan only runs
    once per refresh cycle.
    """
    now = time.time()
    if now - _blocks_per_worker_cache["ts"] < _BLOCKS_WORKER_CACHE_SECS:
        return _blocks_per_worker_cache["data"]

    try:
        r = subprocess.run(
            ["docker", "exec", "pool-db", "psql",
             "-U", "test", "-d", "pool", "-tA", "-F", "|", "-c",
             "SELECT hash, created_at FROM blocks ORDER BY created_at"],
            capture_output=True, text=True, timeout=15)
        block_times = []
        for ln in r.stdout.strip().splitlines():
            parts = ln.split("|")
            if len(parts) >= 2:
                bh = parts[0].strip()
                ts_raw = parts[1].strip()
                ts_dt = None
                for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                    try:
                        ts_dt = datetime.strptime(ts_raw, fmt)
                        break
                    except ValueError:
                        pass
                if ts_dt:
                    block_times.append((bh, ts_dt))
    except Exception:
        return _blocks_per_worker_cache["data"]

    finders = _parse_block_finders(block_times)   # {hash: worker_string}

    counts: dict = {}
    for worker in finders.values():
        counts[worker] = counts.get(worker, 0) + 1

    _blocks_per_worker_cache.update({"data": counts, "ts": now})
    return counts


def _env_read():
    """Read INSTALL_DIR/.env as an ordered {key: raw_value} dict (comments skipped)."""
    result = {}
    if not ENV_FILE.exists():
        return result
    for line in ENV_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        k, _, v = stripped.partition("=")
        result[k.strip()] = v  # value kept verbatim (may be empty)
    return result


def _env_write_keys(updates: dict):
    """Update specific keys in INSTALL_DIR/.env in-place; append any key not already present."""
    if not ENV_FILE.exists():
        ENV_FILE.write_text("", encoding="utf-8")
    lines = ENV_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
    updated = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("#") and "=" in stripped:
            k = stripped.partition("=")[0].strip()
            if k in updates:
                new_lines.append(f"{k}={updates[k]}")
                updated.add(k)
                continue
        new_lines.append(line)
    for k, v in updates.items():
        if k not in updated:
            new_lines.append(f"{k}={v}")
    ENV_FILE.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def _managed_peers():
    """Return list of /ip4/... strings from the managed peer file, seeding from PEERS_FILE on first use."""
    if not PEERS_MANAGED_FILE.exists():
        seed = []
        if PEERS_FILE.exists():
            seed = [l.strip() for l in PEERS_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
                    if l.strip().startswith("/ip4/")]
        PEERS_MANAGED_FILE.write_text("\n".join(seed))
        return seed
    return [l.strip() for l in PEERS_MANAGED_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
            if l.strip().startswith("/ip4/")]


def _append_alert_history(msg):
    try:
        history = []
        if ALERT_HISTORY_FILE.exists():
            try:
                history = json.loads(ALERT_HISTORY_FILE.read_text())
            except Exception:
                history = []
        history.append({"time": datetime.now().strftime("%Y-%m-%d %H:%M"), "msg": msg})
        history = history[-MAX_ALERT_HISTORY:]
        ALERT_HISTORY_FILE.write_text(json.dumps(history, indent=2))
    except Exception:
        pass


def _load_alert_cfg():
    try:
        return json.loads(ALERTS_FILE.read_text())
    except Exception:
        return {}


def _save_alert_cfg(cfg):
    ALERTS_FILE.write_text(json.dumps(cfg, indent=2))


def _load_watchdog_cfg():
    try:
        return json.loads(WATCHDOG_CFG_FILE.read_text())
    except Exception:
        return {}


def _save_watchdog_cfg(cfg):
    WATCHDOG_CFG_FILE.write_text(json.dumps(cfg, indent=2))


def _send_alert(msg, cfg=None):
    _append_alert_history(msg)
    if cfg is None:
        cfg = _load_alert_cfg()
    sent = False
    discord_url = cfg.get("discord_url", "").strip()
    if discord_url:
        data = json.dumps({"content": msg}).encode()
        req  = urllib.request.Request(
            discord_url, data=data,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        sent = True
    tg_token = cfg.get("telegram_token", "").strip()
    tg_chat  = cfg.get("telegram_chat_id", "").strip()
    if tg_token and tg_chat:
        url  = f"https://api.telegram.org/bot{tg_token}/sendMessage"
        data = json.dumps({"chat_id": tg_chat, "text": msg}).encode()
        req  = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        sent = True
    return sent


def _parse_env(path):
    """Parse a KEY=VALUE env file into a dict."""
    result = {}
    try:
        for line in Path(path).read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            result[k.strip()] = v.strip()
    except Exception:
        pass
    return result

def _write_env_key(path, key, value):
    """Update an existing KEY= line or append it if missing."""
    path = Path(path)
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        updated = False
        for i, line in enumerate(lines):
            if line.startswith(f"{key}=") or line.strip() == f"{key}=":
                lines[i] = f"{key}={value}"
                updated = True
                break
        if not updated:
            lines.append(f"{key}={value}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception as e:
        raise RuntimeError(f"Failed to write {key} to {path}: {e}")

def _rpc_creds():
    user = pass_ = ""
    data = _parse_env(ENV_FILE)
    user  = data.get("NODE_RPC_USER", "")
    pass_ = data.get("NODE_RPC_PASS", "")
    return base64.b64encode(f"{user}:{pass_}".encode()).decode()

def _rpc_call(url, method, params=None):
    creds = _rpc_creds()
    data  = json.dumps(
        {"jsonrpc": "2.0", "method": method,
         "params": params or [], "id": 1}).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json",
                 "Authorization": f"Basic {creds}"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["result"]

def _rpc_direct(method, params=None):
    return _rpc_call(RPC_URL, method, params)

def _container_rpc_url(container_name):
    """Return the internal RPC URL for a named container via docker inspect."""
    r = subprocess.run(
        ["docker", "inspect", "--format",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         container_name],
        capture_output=True, text=True, timeout=5)
    ip = r.stdout.strip()
    return f"http://{ip}:38131" if ip else None


def _alert_loop():
    while True:
        time.sleep(60)
        try:
            cfg = _load_alert_cfg()
            has_discord  = bool(cfg.get("discord_url", "").strip())
            has_telegram = bool(cfg.get("telegram_token", "").strip()
                                and cfg.get("telegram_chat_id", "").strip())
            if not has_discord and not has_telegram:
                continue
            now = time.time()

            # ── Freeze check ──────────────────────────────────────────────────
            if cfg.get("freeze_alert", True):
                r = subprocess.run(
                    ["docker", "logs", "--tail", "30", "asic-pool"],
                    capture_output=True, text=True, timeout=8)
                freeze = "FREEZE DETECTED" in (r.stdout + r.stderr)
                with _alert_lock:
                    st = _alert_state["freeze"]
                    if freeze:
                        if not st["active"] or now - st["last_sent"] > ALERT_COOLDOWN:
                            _send_alert(
                                "\U0001f6a8 BlockDAG FREEZE DETECTED\n"
                                "Pool stuck on same block template.\n"
                                "Fix: docker restart bdag-miner-node-2", cfg)
                            st["active"] = True
                            st["last_sent"] = now
                    else:
                        if st["active"]:
                            _send_alert(
                                "✅ BlockDAG pool freeze resolved.", cfg)
                        st["active"] = False

            # ── Peer count check ──────────────────────────────────────────────
            if cfg.get("peer_alert", True):
                threshold = int(cfg.get("peer_threshold", 5))
                info  = _rpc_direct("getNetworkInfo")
                peers = info.get("totalconnected", 0) if isinstance(info, dict) else 0
                with _alert_lock:
                    st = _alert_state["peers"]
                    if peers < threshold:
                        if not st["active"] or now - st["last_sent"] > ALERT_COOLDOWN:
                            _send_alert(
                                f"⚠️ BlockDAG LOW PEERS: {peers} connected "
                                f"(threshold: {threshold})\n"
                                "Node may be about to lose sync.", cfg)
                            st["active"] = True
                            st["last_sent"] = now
                    else:
                        if st["active"]:
                            _send_alert(
                                f"✅ BlockDAG peers recovered: "
                                f"{peers} connected.", cfg)
                        st["active"] = False

            # ── Auto-add peers when count is low ─────────────────────────────
            if cfg.get("peer_auto_add", False) and cfg.get("peer_alert", True):
                try:
                    threshold = int(cfg.get("peer_threshold", 5))
                    info  = _rpc_direct("getNetworkInfo")
                    count = info.get("totalconnected", 0) if isinstance(info, dict) else 0
                    if count < threshold:
                        global _last_auto_add_time
                        managed = _managed_peers()
                        ok = fail = 0
                        for p in managed:
                            try:
                                _rpc_direct("addPeer", [p])
                                ok += 1
                            except Exception:
                                fail += 1
                        _last_auto_add_time = datetime.now().strftime("%Y-%m-%d %H:%M")
                        print(f"[peers] auto-add fired: peers={count} < {threshold}, "
                              f"added {ok}/{len(managed)} (failed {fail})")
                except Exception as e:
                    print(f"[peers] auto-add error: {e}")

            # ── Node height divergence check ──────────────────────────────────
            try:
                def _parse_height_from_logs(container):
                    r = subprocess.run(
                        ["docker", "logs", "--tail", "50", container],
                        capture_output=True, text=True, timeout=8)
                    logs = r.stdout + r.stderr
                    h = None
                    for m in re.finditer(r"number=([\d,]+)", logs):
                        h = int(m.group(1).replace(",", ""))
                    return h

                h1 = _parse_height_from_logs("bdag-miner-node-1")
                h2 = _parse_height_from_logs("bdag-miner-node-2")
                if h1 is not None and h2 is not None:
                    diff_blocks = abs(h1 - h2)
                    div_threshold = int(cfg.get("divergence_threshold", 10))
                    with _alert_lock:
                        st = _alert_state["divergence"]
                        if diff_blocks > div_threshold:
                            if not st["active"] or now - st["last_sent"] > ALERT_COOLDOWN:
                                _send_alert(
                                    f"⚠️ BlockDAG NODE DIVERGENCE: {diff_blocks} blocks apart\n"
                                    f"node1={h1:,}  node2={h2:,}  (threshold: {div_threshold})", cfg)
                                st["active"] = True
                                st["last_sent"] = now
                        else:
                            if st["active"]:
                                _send_alert(
                                    f"✅ BlockDAG node divergence resolved: diff={diff_blocks} blocks.", cfg)
                            st["active"] = False
            except Exception:
                pass

            # ── Disk space check ──────────────────────────────────────────────
            try:
                usage = shutil.disk_usage(BACKUP_DIR)
                free_pct = usage.free / usage.total * 100 if usage.total else 100
                disk_threshold = float(cfg.get("disk_threshold_pct", 15))
                with _alert_lock:
                    st = _alert_state["disk"]
                    if free_pct < disk_threshold:
                        if not st["active"] or now - st["last_sent"] > ALERT_COOLDOWN:
                            _send_alert(
                                f"⚠️ BlockDAG LOW DISK SPACE: {free_pct:.1f}% free on backup drive\n"
                                f"Free: {usage.free / 1e9:.1f} GB  (threshold: {disk_threshold}%)", cfg)
                            st["active"] = True
                            st["last_sent"] = now
                    else:
                        if st["active"]:
                            _send_alert(
                                f"✅ BlockDAG disk space recovered: {free_pct:.1f}% free.", cfg)
                        st["active"] = False
            except Exception:
                pass

        except Exception as e:
            print(f"[alerts] {e}")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type,Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")

    def do_OPTIONS(self):
        self.send_response(200); self._cors(); self.end_headers()

    def _body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", 0)))

    def _json(self, obj, code=200):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self._cors(); self.end_headers()
        self.wfile.write(data)
        self.wfile.flush()

    def _evm_url(self):
        r = subprocess.run(
            ["docker", "inspect", "--format",
             "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
             "bdag-miner-node-1"],
            capture_output=True, text=True, timeout=5)
        ip = r.stdout.strip()
        return f"http://{ip}:18545" if ip else None

    def do_GET(self):
        try:
            self._do_GET()
        except Exception as e:
            print(f"[GET {self.path}] unhandled: {e}")
            try: self._json({"error": str(e)}, 500)
            except Exception: pass

    def _do_GET(self):
        if self.path in ("/", "/index.html"):
            try:
                content = DASH_FILE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(content)
            except FileNotFoundError:
                self.send_error(404, "blockdag-dashboard.html not found next to server")
        elif self.path == "/containers":
            self._containers_status()
        elif self.path == "/nodeid":
            self._get_nodeid()
        elif self.path == "/syncstate":
            self._get_syncstate()
        elif self.path == "/resources":
            self._get_resources()
        elif self.path == "/nodeinfo":
            self._get_nodeinfo()
        elif self.path.startswith("/poolstats"):
            qs = parse_qs(urlparse(self.path).query)
            wf = (qs.get("wallet") or [""])[0].strip().lower()
            # Basic sanity: wallet must look like an EVM address (0x + 40 hex)
            wf = wf if re.match(r'^0x[0-9a-f]{40}$', wf) else None
            self._get_poolstats(wallet_filter=wf)
        elif self.path == "/minermetrics":
            self._get_minermetrics()
        elif self.path.startswith("/backup/schedule"):
            self._get_backup_schedule()
        elif self.path == "/difficulty":
            self._get_difficulty()
        elif self.path == "/payout/config":
            self._get_payout_config()
        elif self.path == "/backup/config":
            self._get_backup_config()
        elif self.path == "/alerts/config":
            self._json(_load_alert_cfg())
        elif self.path == "/alerts/history":
            self._get_alert_history()
        elif self.path == "/node/heights":
            self._get_node_heights()
        elif self.path == "/peers":
            self._get_peers()
        elif self.path == "/peers/connected":
            self._get_connected_peers()
        elif self.path == "/haproxy/status":
            self._get_haproxy_status()
        elif self.path == "/backup/verify":
            self._verify_backup()
        elif self.path == "/backup/location":
            self._get_backup_location()
        elif self.path == "/backup/browse":
            self._browse_backup_location()
        elif self.path == "/backup/list":
            self._list_backups()
        elif self.path == "/maintenance/config":
            self._get_maintenance_config()
        elif self.path == "/watchdog/config":
            self._get_watchdog_config()
        elif self.path == "/autostart/config":
            self._get_autostart_config()
        elif self.path == "/env/config":
            self._get_env_config()
        elif self.path.startswith("/logs/popup"):
            self._open_logs_popup()
        elif self.path.startswith("/logs"):
            self._get_logs()
        elif self.path == "/update-check":
            self._update_check()
        else:
            self.send_error(404)

    def do_POST(self):
        try:
            self._do_POST()
        except Exception as e:
            print(f"[POST {self.path}] unhandled: {e}")
            try: self._json({"error": str(e)}, 500)
            except Exception: pass

    def _do_POST(self):
        p = self.path
        if   p == "/rpc":             self._proxy_rpc()
        elif p == "/evm":             self._proxy_evm()
        elif p == "/docker/start":    self._docker_action("start")
        elif p == "/docker/stop":     self._docker_action("stop")
        elif p == "/docker/restart":  self._docker_action("restart")
        elif p == "/backup":          self._do_backup()
        elif p == "/backup/schedule/enabled":    self._toggle_backup_enabled()
        elif p.startswith("/backup/schedule"):  self._set_backup_schedule()
        elif p == "/alerts/config":    self._save_alert_config()
        elif p == "/alerts/test":      self._test_alert()
        elif p == "/payout/config":    self._set_payout_config()
        elif p == "/backup/config":    self._set_backup_config()
        elif p == "/backup/location":  self._set_backup_location()
        elif p == "/backup/restore":   self._do_restore()
        elif p == "/setup/tasks":      self._run_setup_tasks()
        elif p == "/peers/add":        self._add_peers()
        elif p == "/peers/clear":      self._clear_peers()
        elif p == "/peers/save":       self._save_peers()
        elif p == "/maintenance/config": self._set_maintenance_config()
        elif p == "/watchdog/config":   self._save_watchdog_config()
        elif p == "/autostart/config":  self._save_autostart_config()
        elif p == "/env/config":        self._save_env_config()
        elif p == "/update":           self._do_update()
        elif p == "/restart-server":   self._restart_server()
        else:                          self.send_error(404)

    def _proxy_rpc(self):
        body = self._body()
        auth = self.headers.get("Authorization", "")
        req  = urllib.request.Request(
            RPC_URL, data=body,
            headers={"Content-Type": "application/json", "Authorization": auth})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                data = r.read()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._cors(); self.end_headers()
            self.wfile.write(data)
        except urllib.error.HTTPError as e:
            data = e.read()
            self.send_response(e.code)
            self.send_header("Content-Type", "application/json")
            self._cors(); self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self._json({"error": str(e)}, 502)

    def _docker_action(self, action):
        body = {}
        try:
            body = json.loads(self._body()) if int(self.headers.get("Content-Length", 0)) > 0 else {}
        except Exception:
            pass
        target = body.get("container")
        targets = [target] if target and target in CONTAINERS else CONTAINERS
        results = {}
        for c in targets:
            try:
                r = subprocess.run(["docker", action, c],
                                   capture_output=True, text=True, timeout=30)
                results[c] = "ok" if r.returncode == 0 else r.stderr.strip()
            except Exception as e:
                results[c] = str(e)
        self._json({"ok": True, "results": results})

    def _proxy_evm(self):
        # The EVM HTTP port (18545) is not exposed to the Windows host — only
        # accessible inside the Docker bridge network.  Route through the
        # rpc-failover container (haproxy:2.9-alpine, has busybox wget) which
        # is already on the pool-net and can reach bdag-miner-node-1:18545.
        body_bytes = self._body()
        body_str   = body_bytes.decode("utf-8", errors="replace") if isinstance(body_bytes, bytes) else str(body_bytes)
        try:
            r = subprocess.run(
                ["docker", "exec", "rpc-failover",
                 "wget", "-qO-", "--timeout=10",
                 "--post-data=" + body_str,
                 "--header=Content-Type: application/json",
                 "http://bdag-miner-node-1:18545"],
                capture_output=True, timeout=15)
            if not r.stdout:
                self._json({"error": "EVM node returned empty response"}, 502)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._cors(); self.end_headers()
            self.wfile.write(r.stdout)
        except Exception as e:
            self._json({"error": str(e)}, 502)

    def _get_poolstats(self, wallet_filter=None):
        """
        wallet_filter: lowercase 0x-prefixed EVM address string, or None for
        pool-wide totals.  When provided, block counts, rewards, and recent
        blocks are filtered to that wallet's credits only.
        """
        def psql(query):
            r = subprocess.run(
                ["docker", "exec", "pool-db", "psql",
                 "-U", "test", "-d", "pool", "-tA", "-F", "|", "-c", query],
                capture_output=True, text=True, timeout=10)
            if r.returncode != 0:
                raise RuntimeError(r.stderr.strip())
            return [ln.split("|") for ln in r.stdout.strip().splitlines() if ln.strip()]

        try:
            # ── Block counts & rewards by status ─────────────────────────────
            if wallet_filter:
                # Per-wallet: count distinct blocks where this address has a credit;
                # sum the credit amounts (not block.reward which is the pool total).
                block_rows = psql(
                    "SELECT b.status, COUNT(DISTINCT b.hash), "
                    "COALESCE(SUM(c.amount),0) "
                    "FROM blocks b "
                    "JOIN credits c ON c.block_hash = b.hash "
                    f"WHERE LOWER(c.miner_address) = '{wallet_filter}' "
                    "GROUP BY b.status ORDER BY b.status")
            else:
                block_rows = psql(
                    "SELECT status, COUNT(*), COALESCE(SUM(reward),0) "
                    "FROM blocks GROUP BY status ORDER BY status")
            blocks = {}
            total_reward = 0
            for row in block_rows:
                if len(row) >= 3:
                    st, cnt, rwd = row[0], int(row[1]), int(row[2])
                    blocks[st] = {"count": cnt, "bdag": round(rwd / 1e18, 4)}
                    total_reward += rwd
            total_blocks = sum(v["count"] for v in blocks.values())

            # ── Pending credits (unpaid) ──────────────────────────────────────
            cr = psql("SELECT COUNT(*), COALESCE(SUM(amount),0) "
                      "FROM credits WHERE is_paid = FALSE")
            pending_count = int(cr[0][0]) if cr else 0
            pending_bdag  = round(int(cr[0][1]) / 1e18, 4) if cr else 0

            # ── Payouts ───────────────────────────────────────────────────────
            po = psql("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM payouts")
            payout_count = int(po[0][0]) if po else 0
            payout_bdag  = round(int(po[0][1]) / 1e18, 4) if po else 0

            # ── Recent payout history ─────────────────────────────────────────
            payout_history = []
            try:
                ph = psql(
                    "SELECT DISTINCT ON (p.id) "
                    "  p.tx_hash, p.amount, "
                    "  to_char(p.created_at,'MM-DD HH24:MI'), "
                    "  COALESCE(c.miner_address, '') "
                    "FROM payouts p "
                    "LEFT JOIN credits c "
                    "  ON c.amount = p.amount "
                    "  AND c.is_paid = true "
                    "  AND c.miner_address IS NOT NULL "
                    "  AND c.miner_address != '' "
                    "ORDER BY p.id DESC, "
                    "  ABS(EXTRACT(EPOCH FROM (p.created_at - c.created_at))) "
                    "LIMIT 20")
                for row in ph:
                    if len(row) >= 4:
                        payout_history.append({
                            "tx_hash": str(row[0]),
                            "bdag":    round(int(row[1]) / 1e18, 8),
                            "time":    str(row[2]),
                            "wallet":  str(row[3]),
                        })
            except Exception:
                pass

            # ── Active miners (seen in last 24 h) ─────────────────────────────
            am = psql("SELECT COUNT(*) FROM miners "
                      "WHERE last_active > NOW() - INTERVAL '24 hours'")
            active_miners = int(am[0][0]) if am else 0

            # ── 5 most recent blocks ──────────────────────────────────────────
            if wallet_filter:
                rb = psql(
                    "SELECT b.hash, b.height, b.status, "
                    "to_char(b.created_at,'MM-DD HH24:MI'), "
                    "b.created_at, c.miner_address "
                    "FROM blocks b "
                    "JOIN credits c ON c.block_hash = b.hash "
                    f"WHERE LOWER(c.miner_address) = '{wallet_filter}' "
                    "ORDER BY b.created_at DESC LIMIT 5")
            else:
                rb = psql(
                    "SELECT b.hash, b.height, b.status, "
                    "to_char(b.created_at,'MM-DD HH24:MI'), "
                    "b.created_at, "
                    "(SELECT miner_address FROM credits "
                    " WHERE block_hash = b.hash LIMIT 1) "
                    "FROM blocks b ORDER BY b.created_at DESC LIMIT 5")

            # Finder from pool logs (has wallet.workername) or fall back to credits address
            # r[4] is the raw psql TIMESTAMP string — parse it to datetime for comparison
            def _parse_ts(s):
                if not s:
                    return None
                s = str(s).strip()
                for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                    try:
                        return datetime.strptime(s, fmt)
                    except ValueError:
                        pass
                return None
            block_times = [(str(r[0]), _parse_ts(r[4])) for r in rb if len(r) >= 5]
            finders_log = _parse_block_finders(block_times)

            recent = []
            for r in rb:
                if len(r) < 4:
                    continue
                h           = str(r[0])
                finder_log  = finders_log.get(h)
                finder_db   = str(r[5]) if len(r) >= 6 and r[5] else None
                recent.append({
                    "hash":   h,
                    "height": int(r[1]),
                    "status": r[2],
                    "time":   r[3],
                    "finder": finder_log or finder_db,
                })

            # ── Pool health from container logs ───────────────────────────────
            logs = subprocess.run(
                ["docker", "logs", "--tail", "60", "asic-pool"],
                capture_output=True, text=True, timeout=8
            )
            log_text = logs.stdout + logs.stderr
            health = {"freeze": False, "freeze_secs": None,
                      "error": None, "template_height": None}
            for line in reversed(log_text.splitlines()):
                if "FREEZE DETECTED" in line:
                    health["freeze"] = True
                    m = re.search(r"for ([\d.]+) seconds", line)
                    if m: health["freeze_secs"] = int(float(m.group(1)))
                if "Height:" in line and health["template_height"] is None:
                    m = re.search(r"Height:\s+(\d+)", line)
                    if m: health["template_height"] = int(m.group(1))
                if "Failed to get" in line or "error" in line.lower():
                    if health["error"] is None:
                        health["error"] = re.sub(r"^\S+ \S+ ", "", line).strip()
                if health["freeze"] and health["error"] and health["template_height"]:
                    break

            # Shares accepted / rejected (last hour)
            shares_accepted = shares_rejected = 0
            try:
                sh = psql(
                    "SELECT COALESCE(SUM(CASE WHEN is_valid THEN 1 ELSE 0 END),0), "
                    "COALESCE(SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END),0) "
                    "FROM shares WHERE created_at > NOW() - INTERVAL '1 hour'")
                if sh and len(sh[0]) >= 2:
                    shares_accepted = int(float(sh[0][0]))
                    shares_rejected = int(float(sh[0][1]))
            except Exception:
                pass

            # Blocks found in time windows (wallet-filtered when wallet_filter set)
            _wf_join  = (f"JOIN credits c ON c.block_hash = b.hash "
                         f"WHERE LOWER(c.miner_address) = '{wallet_filter}' AND "
                         if wallet_filter else "")
            _wf_where = (f"JOIN credits c ON c.block_hash = b.hash "
                         f"WHERE LOWER(c.miner_address) = '{wallet_filter}'"
                         if wallet_filter else "")
            def _count_blocks(interval):
                """Count distinct blocks in the given interval, wallet-filtered if applicable."""
                if wallet_filter:
                    q = (f"SELECT COUNT(DISTINCT b.hash) FROM blocks b "
                         f"JOIN credits c ON c.block_hash = b.hash "
                         f"WHERE LOWER(c.miner_address) = '{wallet_filter}' "
                         f"AND b.created_at > NOW() - INTERVAL '{interval}'")
                else:
                    q = (f"SELECT COUNT(*) FROM blocks "
                         f"WHERE created_at > NOW() - INTERVAL '{interval}'")
                try:
                    r = psql(q)
                    return int(r[0][0]) if r else 0
                except Exception:
                    return 0

            blocks_hour   = _count_blocks("1 hour")
            blocks_2h     = _count_blocks("2 hours")
            blocks_6h     = _count_blocks("6 hours")
            blocks_12h    = _count_blocks("12 hours")
            blocks_today  = _count_blocks("24 hours")
            blocks_week   = _count_blocks("7 days")
            blocks_month  = _count_blocks("30 days")

            # Per-hour breakdown: 24 clock-aligned 1-hour slots.
            # Uses date_trunc('hour') so boundaries sit on the hour (e.g. 14:00–15:00),
            # not on rolling 60-min windows from NOW().
            # generate_series fills gaps so empty hours always return 0.
            # Returns index 0 = oldest slot (left of chart), index 23 = current partial hour.
            # Also returns blocks_hourly_start_h: the 0-23 hour-of-day of the oldest slot
            # so the frontend can label each bar with the correct clock time.
            try:
                if wallet_filter:
                    inner = (f"SELECT date_trunc('hour', b.created_at) AS hr, "
                             f"COUNT(DISTINCT b.hash) AS cnt "
                             f"FROM blocks b JOIN credits c ON c.block_hash = b.hash "
                             f"WHERE LOWER(c.miner_address) = '{wallet_filter}' "
                             f"AND b.created_at >= date_trunc('hour', NOW()) - INTERVAL '23 hours' "
                             f"GROUP BY hr")
                else:
                    inner = (f"SELECT date_trunc('hour', created_at) AS hr, COUNT(*) AS cnt "
                             f"FROM blocks "
                             f"WHERE created_at >= date_trunc('hour', NOW()) - INTERVAL '23 hours' "
                             f"GROUP BY hr")
                bh_q = (f"SELECT EXTRACT(HOUR FROM gs.h)::int AS hod, COALESCE(b.cnt, 0) AS cnt "
                        f"FROM generate_series("
                        f"  date_trunc('hour', NOW()) - INTERVAL '23 hours',"
                        f"  date_trunc('hour', NOW()),"
                        f"  INTERVAL '1 hour') AS gs(h) "
                        f"LEFT JOIN ({inner}) b ON b.hr = gs.h "
                        f"ORDER BY gs.h")
                rows = psql(bh_q) or []
                blocks_hourly       = [int(r[1]) for r in rows]
                blocks_hourly_start_h = int(rows[0][0]) if rows else 0
            except Exception:
                blocks_hourly         = [0] * 24
                blocks_hourly_start_h = 0

            # Round duration: seconds since last block
            round_secs = None
            try:
                rd = psql(
                    "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))::int FROM blocks")
                if rd and rd[0][0]:
                    round_secs = int(float(rd[0][0]))
            except Exception:
                pass

            # Hashrate estimate from share difficulty (last 10 min).
            # Returns 0 when pool is running but no recent shares (so UI shows
            # "0 MH/s" rather than "—"), None only on query error.
            hashrate_mhs = None
            try:
                hr = psql(
                    "SELECT COALESCE(SUM(difficulty),0) FROM shares "
                    "WHERE created_at > NOW() - INTERVAL '10 minutes' AND is_valid = TRUE")
                if hr:
                    hashrate_mhs = round(float(hr[0][0]) / 600 / 1e6, 4)
            except Exception:
                pass

            # ── Hashrate per wallet from DB shares (last 10 min) ─────────────
            hashrate_by_addr = {}
            try:
                hr_rows = psql(
                    "SELECT address, COALESCE(SUM(difficulty),0) "
                    "FROM shares "
                    "WHERE created_at > NOW() - INTERVAL '10 minutes' AND is_valid = TRUE "
                    "GROUP BY address")
                for hr in hr_rows:
                    if len(hr) >= 2:
                        hashrate_by_addr[str(hr[0]).lower()] = round(float(hr[1]) / 600 / 1e6, 4)
            except Exception:
                pass

            # ── Blocks found per wallet (from credits table) ───────────────────
            blocks_by_addr = {}
            try:
                bc = psql(
                    "SELECT miner_address, COUNT(DISTINCT block_hash) "
                    "FROM credits GROUP BY miner_address")
                for row in bc:
                    if len(row) >= 2 and row[0]:
                        blocks_by_addr[str(row[0]).lower()] = int(row[1])
            except Exception:
                pass

            # ── Per-worker block counts via pool-log matching (cached 5 min) ──
            blocks_by_worker = _blocks_per_worker_from_logs()

            # ── Per-worker list from pool logs (shows real worker names + diff) ─
            miners_list = _parse_pool_workers()
            # Count distinct log-tracked workers per wallet so the DB fallback
            # doesn't assign the wallet-level total to every individual worker row.
            _wallet_worker_count: dict = {}
            for w in miners_list:
                _k = w["address"].lower()
                _wallet_worker_count[_k] = _wallet_worker_count.get(_k, 0) + 1
            for w in miners_list:
                # Prefer the per-worker log-based hashrate from _parse_pool_workers().
                # Fall back to the DB wallet total only when the log gave nothing
                # AND this is the sole worker for that wallet (total == per-worker).
                log_hr = w.get("hashrate_mhs") or 0
                if log_hr <= 0:
                    if _wallet_worker_count.get(w["address"].lower(), 0) == 1:
                        w["hashrate_mhs"] = hashrate_by_addr.get(w["address"].lower(), 0)
                    # else: multiple workers, no recent log shares → can't split wallet
                    # total meaningfully, leave at 0 rather than show an inflated value.
                # Per-worker block count: prefer log-matched data (wallet.workername or
                # bare wallet), fall back to wallet total from credits table.
                _waddr  = w["address"].lower()
                _wname  = (w.get("worker") or "").strip()
                _wkey   = f"{_waddr}.{_wname}" if _wname else _waddr
                w["blocks_found"] = (blocks_by_worker.get(_wkey)
                                     or blocks_by_worker.get(_waddr)
                                     or blocks_by_addr.get(_waddr, 0))

            # ── Fall back to DB miners table if log parse returned nothing ──────
            if not miners_list:
                worker_map = _dagtech_worker_map()
                try:
                    try:
                        mr = psql(
                            "SELECT address, last_active, ip "
                            "FROM miners "
                            "WHERE last_active > NOW() - INTERVAL '24 hours' "
                            "ORDER BY last_active DESC LIMIT 100")
                        has_ip = True
                    except Exception:
                        mr = psql(
                            "SELECT address, last_active "
                            "FROM miners "
                            "WHERE last_active > NOW() - INTERVAL '24 hours' "
                            "ORDER BY last_active DESC LIMIT 100")
                        has_ip = False
                    for row in mr:
                        if len(row) >= 2:
                            addr = str(row[0])
                            worker = worker_map.get(addr.lower(), "")
                            raw_ip = str(row[2]) if has_ip and len(row) >= 3 and row[2] is not None else ""
                            ip = raw_ip.split(":")[0] if raw_ip else ""
                            miners_list.append({
                                "address":      addr,
                                "worker":       worker,
                                "ip":           ip,
                                "difficulty":   0,
                                "last_active":  str(row[1])[:16],
                                "accepted":     0,
                                "rejected":     0,
                                "hashrate_mhs": hashrate_by_addr.get(addr.lower(), 0),
                                "blocks_found": (blocks_by_worker.get(addr.lower())
                                                 or blocks_by_addr.get(addr.lower(), 0)),
                            })
                except Exception:
                    pass

            # ── Merge in remote miners from BDAG_MINER_SCAN_TARGET ────────────
            # Remote miners always appear as distinct entries using their real IP.
            # If the remote miner has a non-empty worker name that matches an
            # existing pool-log entry exactly, patch that entry's IP + hashrate
            # instead of duplicating.  Blank-worker entries are never merged to
            # avoid collisions when both entries have worker="".
            for rm in _parse_remote_miners():
                rm_worker = rm.get("worker", "").strip()
                match = None
                if rm_worker:   # only merge when a specific worker name is set
                    match = next(
                        (m for m in miners_list
                         if m["address"].lower() == rm["address"].lower()
                         and m.get("worker", "").strip() == rm_worker),
                        None)
                if match:
                    if rm["hashrate_mhs"] > 0:
                        match["hashrate_mhs"] = rm["hashrate_mhs"]
                    match["ip"] = rm["ip"]   # replace Docker bridge IP with real IP
                    if rm["accepted"]:
                        match["accepted"] = rm["accepted"]
                else:
                    _raddr = rm["address"].lower()
                    _rname = (rm.get("worker") or "").strip()
                    _rkey  = f"{_raddr}.{_rname}" if _rname else _raddr
                    rm["blocks_found"] = (blocks_by_worker.get(_rkey)
                                          or blocks_by_worker.get(_raddr)
                                          or blocks_by_addr.get(_raddr, 0))
                    miners_list.append(rm)

            try:
                starting_pdiff = float(_env_read().get("POOL_STARTING_PDIFF") or 0) or None
            except Exception:
                starting_pdiff = None

            self._json({
                "total_blocks":   total_blocks,
                "blocks":         blocks,
                "total_reward_bdag": round(total_reward / 1e18, 4),
                "pending_credits":   {"count": pending_count, "bdag": pending_bdag},
                "payouts":           {"count": payout_count,  "bdag": payout_bdag},
                "active_miners":  len(miners_list),
                "recent_blocks":  recent,
                "health":         health,
                "shares_accepted": shares_accepted,
                "shares_rejected": shares_rejected,
                "blocks_hour":     blocks_hour,
                "blocks_2h":       blocks_2h,
                "blocks_6h":       blocks_6h,
                "blocks_12h":      blocks_12h,
                "blocks_today":    blocks_today,
                "blocks_week":     blocks_week,
                "blocks_month":    blocks_month,
                "blocks_hourly":         blocks_hourly,
                "blocks_hourly_start_h": blocks_hourly_start_h,
                "round_secs":      round_secs,
                "hashrate_mhs":    hashrate_mhs,
                "miners":          miners_list,
                "payout_history":  payout_history,
                "starting_pdiff":  starting_pdiff,
            })
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _get_resources(self):
        result = {}

        # ── Disk: chain data sizes + free space ──────────────────────────────
        def dir_size_gb(path):
            total = 0
            try:
                for f in Path(path).rglob("*"):
                    if f.is_file():
                        total += f.stat().st_size
            except Exception:
                pass
            return round(total / 1e9, 2)

        result["node1_gb"]  = dir_size_gb(NODE1_DATA)
        result["node2_gb"]  = dir_size_gb(NODE2_DATA)
        try:
            usage = shutil.disk_usage(NODE1_DATA)
            result["disk_free_gb"]  = round(usage.free  / 1e9, 1)
            result["disk_total_gb"] = round(usage.total / 1e9, 1)
        except Exception:
            result["disk_free_gb"] = result["disk_total_gb"] = None

        try:
            bu = shutil.disk_usage(_get_backup_dir())
            result["backup_disk_free_gb"]  = round(bu.free  / 1e9, 1)
            result["backup_disk_free_pct"] = round(bu.free / bu.total * 100, 1) if bu.total else None
        except Exception:
            result["backup_disk_free_gb"]  = None
            result["backup_disk_free_pct"] = None

        # ── Container uptime + memory (docker stats --no-stream) ─────────────
        uptimes = {}
        mem     = {}
        for c in CONTAINERS:
            try:
                r = subprocess.run(
                    ["docker", "inspect", "--format",
                     "{{.State.StartedAt}}\t{{.State.Status}}", c],
                    capture_output=True, text=True, timeout=5)
                if r.returncode == 0:
                    started_str, status = r.stdout.strip().split("\t")
                    # started_str: 2026-05-15T12:00:00.000000000Z
                    started = datetime.fromisoformat(
                        started_str.replace("Z", "+00:00").split(".")[0] + "+00:00")
                    secs = int((datetime.now(timezone.utc) - started).total_seconds())
                    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
                    uptimes[c] = f"{h}h {m}m {s}s"
            except Exception:
                uptimes[c] = None

            try:
                r = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format",
                     "{{.MemUsage}}", c],
                    capture_output=True, text=True, timeout=8)
                mem[c] = r.stdout.strip() if r.returncode == 0 else None
            except Exception:
                mem[c] = None

        result["uptimes"] = uptimes
        result["mem"]     = mem

        # Docker volumes disk usage
        try:
            r = subprocess.run(
                ["docker", "system", "df", "--format", "{{json .}}"],
                capture_output=True, text=True, timeout=15)
            for line in r.stdout.strip().splitlines():
                try:
                    obj = json.loads(line)
                    if "Volume" in obj.get("Type", ""):
                        result["docker_vol_size"] = obj.get("Size")
                        break
                except Exception:
                    pass
        except Exception:
            pass

        # Network I/O per container
        net_io = {}
        for c in ["bdag-miner-node-1", "bdag-miner-node-2", "asic-pool"]:
            try:
                r = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{.NetIO}}", c],
                    capture_output=True, text=True, timeout=8)
                net_io[c] = r.stdout.strip() if r.returncode == 0 else None
            except Exception:
                net_io[c] = None
        result["net_io"] = net_io

        # Host network speed — physical NIC bytes/sec via WMI perf counters.
        # Win32_PerfFormattedData_Tcpip_NetworkInterface returns kernel-maintained
        # rate counters instantly (no sampling delay).  Filter out loopback,
        # Docker bridge (vEthernet), tunnels, and virtual miniports so only real
        # physical NICs (Ethernet / Wi-Fi) contribute to the total.
        try:
            ps_cmd = (
                "$nics = Get-CimInstance Win32_PerfFormattedData_Tcpip_NetworkInterface "
                "-ErrorAction SilentlyContinue | Where-Object { "
                "$_.Name -notmatch 'Loopback|Teredo|isatap|vEthernet|6to4|Miniport' }; "
                "$rx = ($nics | Measure-Object BytesReceivedPerSec -Sum).Sum; "
                "$tx = ($nics | Measure-Object BytesSentPerSec -Sum).Sum; "
                "\"$rx,$tx\""
            )
            r = subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_cmd],
                capture_output=True, text=True, timeout=8)
            if r.returncode == 0 and "," in r.stdout.strip():
                rx_s, tx_s = r.stdout.strip().split(",", 1)
                result["net_host_rx_bps"] = int(float(rx_s.strip()))
                result["net_host_tx_bps"] = int(float(tx_s.strip()))
            else:
                result["net_host_rx_bps"] = result["net_host_tx_bps"] = None
        except Exception:
            result["net_host_rx_bps"] = result["net_host_tx_bps"] = None

        # Local IP address
        try:
            import socket as _socket
            s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
            s.settimeout(0)
            try:
                s.connect(("8.8.8.8", 80))
                result["local_ip"] = s.getsockname()[0]
            finally:
                s.close()
        except Exception:
            result["local_ip"] = None

        self._json(result)

    def _get_minermetrics(self):
        result = {}

        # ── 1. Find config.env ────────────────────────────────────────────────
        config = {}
        config_path = None
        for p in _DAGTECH_CONFIG_PATHS:
            if p.exists():
                config = _parse_env(p)
                config_path = p
                break

        if config:
            if config.get("WALLET"):
                result["wallet_full"] = config["WALLET"]
            if config.get("WORKER_NAME"):
                result["worker_name"] = config["WORKER_NAME"]

        # ── 2. Try the miner's built-in HTTP metrics endpoint ─────────────────
        metrics_port = int(config.get("METRICS_PORT", 8880))
        http_ok = False
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{metrics_port}/metrics",
                headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=2) as r:
                result.update(json.loads(r.read()))
            http_ok = True
        except Exception:
            pass

        # ── 3. Control server (8880) — status + sysinfo ───────────────────────
        ctrl = "http://127.0.0.1:8880"
        try:
            req = urllib.request.Request(f"{ctrl}/status")
            with urllib.request.urlopen(req, timeout=2) as r:
                st = json.loads(r.read())
            result["running"] = st.get("running", False)
        except Exception:
            result["running"] = False

        try:
            req = urllib.request.Request(f"{ctrl}/sysinfo")
            with urllib.request.urlopen(req, timeout=3) as r:
                result.update(json.loads(r.read()))
        except Exception:
            pass

        # ── 4. Log file fallback — hashrate, shares, difficulty ───────────────
        if not http_ok and config_path:
            try:
                log_dir = config_path.parent / "logs"
                log_file = log_dir / f"miner_{datetime.now().strftime('%Y-%m-%d')}.log"
                if log_file.exists():
                    lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
                    # Scan from the end for the most recent stats/difficulty lines
                    found_stats = found_diff = False
                    for line in reversed(lines):
                        if not found_stats:
                            # "[DagTech] 36640.3 H/s | Shares: 368/368/0 (sub/acc/rej) | Uptime: 0h5m"
                            m = re.search(
                                r'\[DagTech\]\s+([\d.]+)\s+H/s\s+\|\s+Shares:\s+(\d+)/(\d+)/(\d+)', line)
                            if m:
                                result["hashrate"] = float(m.group(1))   # H/s
                                result["accepted"] = int(m.group(3))
                                result["rejected"] = int(m.group(4))
                                found_stats = True
                        if not found_diff:
                            # "[DagTech] Difficulty: 1.06862202"
                            m = re.search(r'\[DagTech\]\s+Difficulty:\s+([\d.]+)', line)
                            if m:
                                result["difficulty"] = float(m.group(1))
                                found_diff = True
                        if found_stats and found_diff:
                            break
            except Exception:
                pass

        if not result:
            self._json({"error": "Miner not found — check config path and control server"}, 502)
            return

        self._json(result)

    def _get_difficulty(self):
        diff = None
        block_time = None
        try:
            best_hash = _rpc_direct("getBestBlockHash")
            if not best_hash:
                raise RuntimeError("no best block hash")
            timestamps = []
            cur_hash = best_hash
            for _ in range(11):  # 11 blocks → 10 gaps for avg block time
                block = _rpc_direct("getBlock", [cur_hash, True])
                if not isinstance(block, dict):
                    break
                if diff is None and "difficulty" in block:
                    diff = block["difficulty"]
                ts = block.get("timestamp") or block.get("time")
                if ts:
                    if isinstance(ts, str):
                        try:
                            t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            timestamps.append(t.timestamp())
                        except Exception:
                            pass
                    else:
                        timestamps.append(float(ts))
                parents = block.get("parents") or []
                null_hash = "0x" + "0" * 64
                next_hash = next(
                    (p for p in parents if p and p not in ("null", null_hash)), None)
                if not next_hash:
                    break
                cur_hash = next_hash
            if len(timestamps) >= 2:
                timestamps.sort(reverse=True)
                gaps = [timestamps[i] - timestamps[i+1]
                        for i in range(len(timestamps)-1)
                        if timestamps[i] > timestamps[i+1]]
                if gaps:
                    block_time = round(sum(gaps) / len(gaps), 1)
        except Exception:
            pass
        self._json({"difficulty": diff, "avg_block_time_secs": block_time})

    def _get_payout_config(self):
        try:
            cfg = json.loads(PAYOUT_FILE.read_text()) if PAYOUT_FILE.exists() else {}
        except Exception:
            cfg = {}
        self._json({"min_payout": cfg.get("min_payout", 1.0)})

    def _set_payout_config(self):
        try:
            body = json.loads(self._body())
            cfg  = {"min_payout": float(body.get("min_payout", 1.0))}
            PAYOUT_FILE.write_text(json.dumps(cfg))
            self._json({"ok": True})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_backup_config(self):
        try:
            cfg = json.loads(BACKUP_CFG_FILE.read_text()) if BACKUP_CFG_FILE.exists() else {}
        except Exception:
            cfg = {}
        self._json({
            "retain_copies_1": cfg.get("retain_copies_1", 12),
            "retain_copies_2": cfg.get("retain_copies_2", 12),
        })

    def _set_backup_config(self):
        try:
            body = json.loads(self._body())
            try:
                cfg = json.loads(BACKUP_CFG_FILE.read_text()) if BACKUP_CFG_FILE.exists() else {}
            except Exception:
                cfg = {}
            updated = {}
            for key in ("retain_copies_1", "retain_copies_2"):
                if key in body:
                    val = int(body[key])
                    if val < 1:
                        self._json({"ok": False, "error": f"{key} must be >= 1"}, 400)
                        return
                    updated[key] = val
            if not updated:
                self._json({"ok": False, "error": "no valid keys provided"}, 400)
                return
            cfg.update(updated)
            BACKUP_CFG_FILE.write_text(json.dumps(cfg))
            self._json({"ok": True, **updated})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_backup_location(self):
        self._json({"path": _get_backup_dir()})

    def _set_backup_location(self):
        try:
            body = json.loads(self._body())
            path = (body.get("path") or "").strip()
            if not path:
                self._json({"ok": False, "error": "Path is required"}); return
            os.makedirs(path, exist_ok=True)
            cfg = json.loads(BACKUP_CFG_FILE.read_text()) if BACKUP_CFG_FILE.exists() else {}
            cfg["backup_dir"] = path
            BACKUP_CFG_FILE.write_text(json.dumps(cfg))
            self._json({"ok": True})
        except Exception as e:
            self._json({"ok": False, "error": str(e)})

    def _browse_backup_location(self):
        """Open a native Windows folder picker and return the chosen path."""
        try:
            r = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 "Add-Type -AssemblyName System.Windows.Forms | Out-Null;"
                 "$dlg = New-Object System.Windows.Forms.FolderBrowserDialog;"
                 "$dlg.Description = 'Select backup folder';"
                 "$dlg.ShowNewFolderButton = $true;"
                 # Create a hidden top-most owner form so the dialog appears in front
                 "$owner = New-Object System.Windows.Forms.Form;"
                 "$owner.TopMost = $true;"
                 "$owner.ShowInTaskbar = $false;"
                 "$owner.WindowState = 'Minimized';"
                 "$owner.Show();"
                 "$owner.Hide();"
                 "$result = $dlg.ShowDialog($owner);"
                 "$owner.Dispose();"
                 "if ($result -eq [System.Windows.Forms.DialogResult]::OK)"
                 "  { $dlg.SelectedPath } else { '::cancelled::' }"],
                capture_output=True, text=True, timeout=120)
            out = r.stdout.strip()
            if not out or out == "::cancelled::":
                self._json({"cancelled": True})
            else:
                self._json({"path": out})
        except Exception as e:
            self._json({"error": str(e)})

    def _list_backups(self):
        try:
            backup_path = Path(_get_backup_dir())
            if not backup_path.exists():
                self._json({"backups": []}); return
            dirs = sorted(
                [d for d in backup_path.iterdir()
                 if d.is_dir() and d.name.startswith("blockdag-backup-")],
                key=lambda d: d.name, reverse=True)
            backups = []
            for d in dirs:
                try:
                    ts_str = d.name.replace("blockdag-backup-", "")
                    dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                    label = dt.strftime("%Y-%m-%d  %H:%M:%S")
                except Exception:
                    label = d.name
                backups.append({"name": d.name, "label": label})
            self._json({"backups": backups})
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _do_restore(self):
        try:
            body = json.loads(self._body())
            backup_name = (body.get("backup") or "").strip()
            if not backup_name:
                self._json({"ok": False, "error": "No backup specified"}); return
            backup_path = Path(_get_backup_dir()) / backup_name
            if not backup_path.exists():
                self._json({"ok": False, "error": f"Backup not found: {backup_name}"}); return
            # Stop the full stack
            subprocess.run(["docker", "compose", "down"],
                           capture_output=True, timeout=120,
                           cwd=str(INSTALL_DIR))
            # Replace chain data for each node present in the backup
            for node, dst_str in [("node1", NODE1_DATA), ("node2", NODE2_DATA)]:
                src = backup_path / node
                if not src.exists():
                    continue
                dst = Path(dst_str)
                if dst.exists():
                    shutil.rmtree(dst)
                shutil.copytree(str(src), str(dst))
            # Restart the stack
            subprocess.run(["docker", "compose", "up", "-d"],
                           capture_output=True, timeout=120,
                           cwd=str(INSTALL_DIR))
            self._json({"ok": True})
        except Exception as e:
            self._json({"ok": False, "error": str(e)})

    def _run_setup_tasks(self):
        setup_script = str(Path(__file__).parent / "setup-tasks.ps1")
        try:
            out, err, rc = self._ps(f'& "{setup_script}"')
            if rc == 0:
                self._json({"ok": True, "output": out.strip()})
            else:
                self._json({"ok": False, "error": (err or out).strip()}, 500)
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_nodeinfo(self):
        result = {}
        try:
            r = subprocess.run(
                ["docker", "inspect", "--format", "{{.Config.Image}}", "bdag-miner-node-1"],
                capture_output=True, text=True, timeout=5)
            result["version"] = r.stdout.strip() if r.returncode == 0 else None
        except Exception:
            result["version"] = None
        try:
            env = _env_read()
            result["installer_version"] = env.get("INSTALLER_VERSION") or None
        except Exception:
            result["installer_version"] = None
        self._json(result)

    # ── Software update ───────────────────────────────────────────────────────

    def _update_check(self):
        GH_VER = "https://raw.githubusercontent.com/danvandamme/blockdag-node-installer/main/VERSION"
        try:
            with urllib.request.urlopen(GH_VER, timeout=8) as r:
                latest = r.read().decode(errors="replace").strip()
            current = (_env_read().get("INSTALLER_VERSION") or "unknown")
            self._json({"current": current, "latest": latest, "up_to_date": current == latest})
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _do_update(self):
        GH_BASE  = "https://raw.githubusercontent.com/danvandamme/blockdag-node-installer/main"
        DASH_DIR = Path(__file__).parent
        try:
            with urllib.request.urlopen(f"{GH_BASE}/VERSION", timeout=10) as r:
                latest = r.read().decode(errors="replace").strip()
            current = (_env_read().get("INSTALLER_VERSION") or "unknown")
            if current == latest:
                self._json({"ok": True, "status": "up_to_date", "version": current})
                return
            downloads = [
                ("dashboard/blockdag-dashboard.html",       DASH_DIR / "blockdag-dashboard.html"),
                ("dashboard/blockdag-dashboard-server.py",  DASH_DIR / "blockdag-dashboard-server.py.new"),
            ]
            errors = []
            for src, dst in downloads:
                try:
                    with urllib.request.urlopen(f"{GH_BASE}/{src}", timeout=60) as r:
                        data = r.read()
                    dst.write_bytes(data)
                    print(f"[Update] Downloaded {src}")
                except Exception as e:
                    errors.append(f"{src}: {e}")
                    print(f"[Update] Failed {src}: {e}")
            try:
                _env_write_keys({"INSTALLER_VERSION": latest})
            except Exception as e:
                errors.append(f".env: {e}")
            has_ctrl = (DASH_DIR / "blockdag-dashboard-server.py.new").exists()
            print(f"[Update] Applied: {current} -> {latest}")
            self._json({
                "ok": True, "status": "updated",
                "from": current, "to": latest,
                "restart_required": has_ctrl,
                "errors": "; ".join(errors) if errors else None,
            })
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _restart_server(self):
        import sys as _sys, os as _os, shutil as _shutil, subprocess as _sub, time as _time
        self._json({"ok": True})
        DASH_DIR = Path(__file__).parent
        pending  = DASH_DIR / "blockdag-dashboard-server.py.new"
        script   = Path(__file__)
        if pending.exists():
            try:
                _shutil.move(str(pending), str(script))
                print("[Update] Pending server update applied.")
            except Exception as e:
                print(f"[Update] Warning: could not apply pending update: {e}")
        _time.sleep(0.4)
        try:
            _sub.Popen(
                [_sys.executable, str(script)],
                creationflags=0x00000208,   # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
                close_fds=True,
            )
        except Exception as e:
            print(f"[Update] Could not spawn new server: {e}")
        _os._exit(0)

    def _node_syncstate(self, container, peer_max_height=None):
        """Return sync state for a node by parsing its docker logs."""
        result = {"container": container}
        started_dt = None
        try:
            # Get container metadata: start time, status, image tag
            ri = subprocess.run(
                ["docker", "inspect", "--format",
                 "{{.State.StartedAt}}\t{{.State.Status}}\t{{.Config.Image}}", container],
                capture_output=True, text=True, timeout=5)
            if ri.returncode == 0:
                iparts = ri.stdout.strip().split("\t")
                try:
                    started_dt = datetime.fromisoformat(
                        iparts[0].replace("Z", "+00:00").split(".")[0] + "+00:00")
                    secs = int((datetime.now(timezone.utc) - started_dt).total_seconds())
                    h, m2, s = secs // 3600, (secs % 3600) // 60, secs % 60
                    result["uptime"] = f"{h}h {m2}m {s}s"
                except Exception:
                    pass
                if len(iparts) >= 2:
                    result["container_status"] = iparts[1].strip()
        except Exception:
            pass

        try:
            r = subprocess.run(
                ["docker", "logs", "--tail", "400", container],
                capture_output=True, text=True, timeout=10)
            # Strip ANSI escape codes that Docker logs embed around field names
            logs = re.sub(r'\x1b\[[0-9;]*m|\[0m', '', r.stdout + r.stderr)

            # Parse version from recent logs (auto-detected on every refresh).
            # Node ID is NOT scanned here — it is populated on demand via the
            # /nodeid endpoint and cached; we just pass the cached value through.
            version = None
            for line in logs.splitlines():
                if not version:
                    m = re.search(r"BDAG Version=(\S+)", line)
                    if m:
                        version = m.group(1)
                        break

            if version:
                _node_version_cache[container] = version
            elif _node_version_cache.get(container):
                version = _node_version_cache[container]

            # Version only appears at startup — scan first 5 min if still missing
            if not version and started_dt is not None:
                try:
                    since_str = started_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    until_str = (started_dt + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    rs = subprocess.run(
                        ["docker", "logs", "--since", since_str, "--until", until_str, container],
                        capture_output=True, text=True, timeout=10)
                    sl = re.sub(r'\x1b\[[0-9;]*m|\[0m', '', rs.stdout + rs.stderr)
                    for line in sl.splitlines():
                        if not version:
                            m = re.search(r"BDAG Version=(\S+)", line)
                            if m:
                                version = m.group(1)
                                _node_version_cache[container] = version
                                break
                except Exception:
                    pass

            # Node ID: serve from cache only (populated by explicit /nodeid request)
            result["node_id"] = _node_id_cache.get(container)
            result["version"] = version

            block_age_secs = None
            cur_height = None

            # Node1 DAG format: "Loaded most recent local block  number=7,189,129  age=3s"
            age_str = None
            for m in re.finditer(
                    r"Loaded most recent local block\s+\S*number=([\d,]+)\s+\S*age=(\S+)", logs):
                cur_height     = int(m.group(1).replace(",", ""))
                age_str        = m.group(2)
                block_age_secs = self._parse_dur(age_str)

            # EVM format (both nodes): "Imported new chain segment  number=1,748,333  age=2mo1w11h"
            # Capture block number from every such line, and age when present (absent near tip).
            for m in re.finditer(r"Imported new chain segment\s+.*?number=([\d,]+)", logs):
                cur_height = int(m.group(1).replace(",", ""))
            for m in re.finditer(r"Imported new chain segment\s+.*?number=([\d,]+).*?age=(\S+)", logs):
                cur_height     = int(m.group(1).replace(",", ""))
                age_str        = m.group(2)
                block_age_secs = self._parse_dur(age_str)

            # DAG sync format: "Syncing graph state  cur=(order,height,...) target=(order,height,...)"
            tgt_height = None
            for m in re.finditer(
                    r"Syncing graph state.*?cur=\(\d+,(\d+),.*?target=\(\d+,(\d+),", logs):
                cur_height = int(m.group(1))
                tgt_height = int(m.group(2))

            # Fall back to peer max height as target
            if not tgt_height and peer_max_height:
                tgt_height = peer_max_height

            synced = block_age_secs is not None and block_age_secs < 60
            pct = None
            if not synced and cur_height and tgt_height and tgt_height > 0:
                pct = round(min(99.9, cur_height / tgt_height * 100), 1)
            elif synced:
                pct = 100

            result.update({
                "synced":         synced,
                "pct":            pct,
                "block_age_secs": block_age_secs,
                "age_str":        age_str,
                "cur_height":     cur_height,
                "tgt_height":     tgt_height,
            })
        except Exception as e:
            result["error"] = str(e)
        return result

    def _get_syncstate(self):
        try:
            # Get peer max height and network info via HAProxy RPC
            peer_max = None
            network = None
            total_peers = None
            try:
                peers = _rpc_direct("getPeerInfo")
                if isinstance(peers, list):
                    # Use peer list length as a reliable fallback count
                    total_peers = len(peers)
                    if peers:
                        heights = [p.get("graphstate", {}).get("mainheight", 0) or 0 for p in peers]
                        peer_max = max(heights) if heights else None
            except Exception:
                pass
            try:
                net_info = _rpc_direct("getNetworkInfo")
                if isinstance(net_info, dict):
                    # Try the top-level field first, then fall back to infos[0].connecteds
                    ni = net_info.get("totalconnected")
                    if ni is None:
                        infos_list = net_info.get("infos", [])
                        if infos_list:
                            ni = infos_list[0].get("connecteds")
                    if ni is not None:
                        total_peers = int(ni)  # authoritative count overrides getPeerInfo length
                    infos = net_info.get("infos", [])
                    if infos:
                        network = infos[0].get("name")
            except Exception:
                pass

            results = {}
            def _fetch(name):
                results[name] = self._node_syncstate(name, peer_max)
            t1 = threading.Thread(target=_fetch, args=("bdag-miner-node-1",))
            t2 = threading.Thread(target=_fetch, args=("bdag-miner-node-2",))
            t1.start(); t2.start()
            t1.join(timeout=15); t2.join(timeout=15)
            node1 = results.get("bdag-miner-node-1", {"error": "timeout"})
            node2 = results.get("bdag-miner-node-2", {"error": "timeout"})

            # Top-level fields reflect whichever node is ahead (the RPC primary, node1)
            primary = node1 if node1.get("cur_height") else node2
            self._json({
                "node1":           node1,
                "node2":           node2,
                "network":         network,
                "total_peers":     total_peers,
                # Legacy flat fields for backwards compat
                "synced":          primary.get("synced", False),
                "pct":             primary.get("pct"),
                "cur_height":      primary.get("cur_height"),
                "tgt_height":      primary.get("tgt_height"),
                "block_age_secs":  primary.get("block_age_secs"),
            })
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _parse_dur(self, s):
        total = 0
        for m in re.finditer(r"(\d+)(mo|w|d|h|m|s)", s):
            v, u = int(m.group(1)), m.group(2)
            if   u == "mo": total += v * 2592000
            elif u == "w":  total += v * 604800
            elif u == "d":  total += v * 86400
            elif u == "h":  total += v * 3600
            elif u == "m":  total += v * 60
            else:           total += v
        return total

    def _get_nodeid(self):
        try:
            force = "force=1" in self.path
            result = {"id": None, "id2": None}
            for key, container in [("id", "bdag-miner-node-1"), ("id2", "bdag-miner-node-2")]:
                nid = self._resolve_nodeid(container, force=force)
                result[key] = nid
            result["cached"] = not force
            self._json(result)
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _resolve_nodeid(self, container, force=False):
        # Return cached value unless caller wants a fresh scan
        if not force:
            cached = _node_id_cache.get(container)
            if cached:
                return cached
        else:
            _node_id_cache.pop(container, None)

        # Derive the libp2p peer ID directly from the node's network.key file.
        # The key is stored as 64 ASCII hex chars (32-byte secp256k1 private key).
        # Derivation: privkey → secp256k1 compressed pubkey → protobuf PublicKey
        #             → identity multihash → base58btc = the peer ID.
        # This is deterministic and works regardless of log format changes.
        try:
            r = subprocess.run(
                ["docker", "exec", container, "cat", "/data/mainnet/network.key"],
                capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                hex_key = r.stdout.strip()
                nid = _derive_peer_id(hex_key)
                if nid:
                    _node_id_cache[container] = nid
                    return nid
        except Exception:
            pass

        return None

    def _containers_status(self):
        results = {}
        for c in CONTAINERS:
            try:
                r = subprocess.run(
                    ["docker", "inspect", "--format", "{{.State.Status}}", c],
                    capture_output=True, text=True, timeout=10)
                results[c] = r.stdout.strip() if r.returncode == 0 else "not found"
            except Exception:
                results[c] = "error"
        self._json(results)

    def _ps(self, script):
        r = subprocess.run(
            ["powershell", "-NonInteractive", "-Command", script],
            capture_output=True, text=True, timeout=20)
        return r.stdout.strip(), r.stderr.strip(), r.returncode

    def _task_name_from_qs(self):
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        n  = qs.get("task", ["1"])[0]
        return "BlockDAG-Backup" if n == "1" else "BlockDAG-Backup-2"

    def _get_backup_schedule(self):
        task = self._task_name_from_qs()
        # Convert RepetitionInterval to TotalHours inside PowerShell so we get a
        # plain number regardless of whether the interval is stored as an ISO 8601
        # duration ("PT1H") or a .NET TimeSpan string ("01:00:00" / "7.00:00:00").
        script = (
            'try {'
            '  $t = Get-ScheduledTask -TaskName "' + task + '" -ErrorAction Stop;'
            '  $i = Get-ScheduledTaskInfo -TaskName "' + task + '";'
            '  $ri = $t.Triggers[0].RepetitionInterval;'
            '  $ts = [TimeSpan]::Zero;'
            '  try { $ts = [Xml.XmlConvert]::ToTimeSpan($ri) } catch {'
            '    try { $ts = [TimeSpan]::Parse($ri) } catch {} };'
            '  $hours = [math]::Round($ts.TotalHours, 4);'
            '  $lr = if ($i.LastRunTime -gt [datetime]"2000-01-01")'
            '    { $i.LastRunTime.ToString("yyyy-MM-dd HH:mm") } else { "Never" };'
            '  $nr = if ($i.NextRunTime -gt [datetime]"2000-01-01")'
            '    { $i.NextRunTime.ToString("yyyy-MM-dd HH:mm") } else { "Unknown" };'
            '  $rc = [int]$i.LastTaskResult;'
            '  $state = $t.State.ToString();'
            '  Write-Output "$hours|$lr|$nr|$rc|$state"'
            '} catch { Write-Output "NOT_FOUND" }'
        )
        out, _, _ = self._ps(script)
        if out == "NOT_FOUND" or not out:
            self._json({"task_exists": False})
            return
        parts = out.split("|")
        try:
            hours = float(parts[0])
        except (ValueError, IndexError):
            hours = 0.0
        state   = parts[4].strip() if len(parts) > 4 else "Ready"
        enabled = state.lower() != "disabled"
        self._json({
            "task_exists":    True,
            "interval_hours": round(hours, 4),
            "last_run":       parts[1] if len(parts) > 1 else "Never",
            "next_run":       parts[2] if len(parts) > 2 else "Unknown",
            "last_result_ok": parts[3].strip() == "0" if len(parts) > 3 else None,
            "enabled":        enabled,
        })

    def _set_backup_schedule(self):
        task = self._task_name_from_qs()
        try:
            body       = json.loads(self._body())
            hours      = float(body.get("interval_hours", 1))
            if hours <= 0:
                self._json({"ok": False, "error": "interval must be > 0"}, 400)
                return
            total_mins = round(hours * 60)
            iso        = "PT" + str(total_mins // 60) + "H" if total_mins % 60 == 0 \
                         else "PT" + str(total_mins) + "M"
            script = (
                '$interval = [TimeSpan]::FromMinutes(' + str(total_mins) + ');'
                '$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date'
                ' -RepetitionInterval $interval'
                ' -RepetitionDuration ([TimeSpan]::MaxValue);'
                'Set-ScheduledTask -TaskName "' + task + '" -Trigger $trigger | Out-Null;'
                'Write-Output "OK"'
            )
            out, err, rc = self._ps(script)
            if rc == 0 and "OK" in out:
                self._json({"ok": True, "interval": iso, "interval_hours": hours})
            else:
                self._json({"ok": False, "error": err or out}, 500)
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _toggle_backup_enabled(self):
        try:
            body    = json.loads(self._body())
            task    = "BlockDAG-Backup" if str(body.get("task", 1)) == "1" else "BlockDAG-Backup-2"
            enable  = bool(body.get("enabled", True))
            action  = "Enable" if enable else "Disable"
            script  = f'{action}-ScheduledTask -TaskName "{task}" -ErrorAction Stop | Out-Null; Write-Output "OK"'
            out, err, rc = self._ps(script)
            if rc == 0 and "OK" in out:
                self._json({"ok": True, "enabled": enable})
            else:
                self._json({"ok": False, "error": err or out}, 500)
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _open_logs_popup(self):
        """Open the last 50 lines of a container's logs in a new PowerShell window,
        then follow (-f) so the output keeps scrolling live."""
        from urllib.parse import urlparse, parse_qs
        qs        = parse_qs(urlparse(self.path).query)
        container = qs.get("container", ["bdag-miner-node-1"])[0]
        if container not in CONTAINERS:
            self._json({"ok": False, "error": "unknown container"}, 400)
            return
        titles = {
            "bdag-miner-node-1": "Node 1 Logs",
            "bdag-miner-node-2": "Node 2 Logs",
            "asic-pool":         "Pool Logs",
        }
        title = titles.get(container, f"{container} Logs")
        cmd   = f"docker logs --tail 50 -f {container}"
        try:
            # Open a new PowerShell console window; -NoExit keeps it open after the
            # command finishes (e.g. if the container stops).
            subprocess.Popen(
                f'start "BlockDAG {title}" powershell -NoProfile -NoExit -Command "{cmd}"',
                shell=True
            )
            self._json({"ok": True})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_logs(self):
        from urllib.parse import urlparse, parse_qs
        qs        = parse_qs(urlparse(self.path).query)
        container = qs.get("container", ["bdag-miner-node-1"])[0]
        try:
            tail = max(1, min(1000, int(qs.get("tail", ["150"])[0])))
        except ValueError:
            tail = 150
        if container not in CONTAINERS:
            self._json({"error": "unknown container"}, 400)
            return
        try:
            r = subprocess.run(
                ["docker", "logs", "--tail", str(tail), "--timestamps", container],
                capture_output=True, text=True, timeout=15)
            lines = (r.stdout + r.stderr).splitlines()
            self._json({"container": container, "lines": lines})
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _save_alert_config(self):
        try:
            cfg = json.loads(self._body())
            _save_alert_cfg(cfg)
            self._json({"ok": True})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _test_alert(self):
        try:
            cfg = _load_alert_cfg()
            sent = _send_alert("🔔 BlockDAG test alert — webhook is working.", cfg)
            self._json({"ok": sent, "error": None if sent else "No webhook configured"})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _do_backup(self):
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = os.path.join(_get_backup_dir(), f"blockdag-backup-{ts}")
        try:
            os.makedirs(dst, exist_ok=True)
            # node1 is the HAProxy backup node — safe to stop without dropping RPC
            subprocess.run(["docker", "stop", "bdag-miner-node-1"],
                           capture_output=True, timeout=60)
            shutil.copytree(NODE1_DATA, os.path.join(dst, "node1"))
            subprocess.run(["docker", "start", "bdag-miner-node-1"],
                           capture_output=True, timeout=60)
            # node2 is RPC primary — copy while running (warm copy, same as hourly snapshot)
            shutil.copytree(NODE2_DATA, os.path.join(dst, "node2"))
            self._json({"ok": True, "path": dst})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_alert_history(self):
        try:
            history = []
            if ALERT_HISTORY_FILE.exists():
                try:
                    history = json.loads(ALERT_HISTORY_FILE.read_text())
                except Exception:
                    history = []
            self._json({"history": list(reversed(history))})
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _get_node_heights(self):
        def _parse_height(container):
            r = subprocess.run(
                ["docker", "logs", "--tail", "50", container],
                capture_output=True, text=True, timeout=8)
            logs = r.stdout + r.stderr
            h = None
            for m in re.finditer(r"number=([\d,]+)", logs):
                h = int(m.group(1).replace(",", ""))
            return h

        try:
            h1 = _parse_height("bdag-miner-node-1")
            h2 = _parse_height("bdag-miner-node-2")
            if h1 is None and h2 is None:
                self._json({"error": "Could not parse heights from logs"}, 500)
                return
            h1 = h1 or 0
            h2 = h2 or 0
            diff = abs(h1 - h2)
            self._json({
                "node1": h1, "node2": h2,
                "diff_blocks": diff,
                "diverged": diff > 10,
            })
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _get_peers(self):
        try:
            peers = _managed_peers()
            # Read which peers are currently applied (written to .env)
            applied = []
            try:
                env = _env_read()
                flags = env.get("NODE1_ADDPEER_FLAGS", "")
                if flags:
                    applied = [f[len("--addpeer="):] for f in flags.split()
                               if f.startswith("--addpeer=")]
            except Exception:
                pass
            self._json({
                "peers":        peers,
                "count":        len(peers),
                "applied":      applied,
                "last_auto_add": _last_auto_add_time,
            })
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _save_peers(self):
        try:
            body = json.loads(self._body())
            raw  = body.get("peers", "")
            if isinstance(raw, list):
                lines = raw
            else:
                lines = raw.splitlines()
            peers = [l.strip() for l in lines if l.strip().startswith("/ip4/")]
            PEERS_MANAGED_FILE.write_text("\n".join(peers))
            self._json({"ok": True, "count": len(peers)})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _add_peers(self):
        """Write managed peer list into .env as NODE1_ADDPEER_FLAGS / NODE2_ADDPEER_FLAGS.
        docker-compose.yml references ${NODE1_ADDPEER_FLAGS:-} / ${NODE2_ADDPEER_FLAGS:-}
        so nodes pick up the flags on next restart — the YAML itself is never modified."""
        try:
            body = {}
            try:
                cl = int(self.headers.get("Content-Length", 0))
                if cl > 0:
                    body = json.loads(self._body())
            except Exception:
                pass
            if "peers" in body and isinstance(body["peers"], list):
                peers = [p.strip() for p in body["peers"] if str(p).strip().startswith("/ip4/")]
            else:
                peers = _managed_peers()

            if not ENV_FILE.exists():
                self._json({"ok": False, "error": f".env not found at {ENV_FILE}"}, 500)
                return

            # Space-separated --addpeer flags (empty string when peer list is empty)
            flags = " ".join(f"--addpeer={p}" for p in peers)
            _env_write_keys({"NODE1_ADDPEER_FLAGS": flags,
                             "NODE2_ADDPEER_FLAGS": flags})
            self._json({"ok": True, "applied": len(peers),
                        "note": f"Written {len(peers)} peer(s) to .env. Restart nodes to apply."})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _clear_peers(self):
        """Clear NODE1_ADDPEER_FLAGS and NODE2_ADDPEER_FLAGS in .env."""
        try:
            if not ENV_FILE.exists():
                self._json({"ok": False, "error": f".env not found at {ENV_FILE}"}, 500)
                return
            _env_write_keys({"NODE1_ADDPEER_FLAGS": "",
                             "NODE2_ADDPEER_FLAGS": ""})
            self._json({"ok": True, "removed": 0,
                        "note": "Peer flags cleared from .env. Restart nodes to apply."})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_connected_peers(self):
        """Return live peer list from the node via getPeerInfo RPC."""
        try:
            result = _rpc_direct("getPeerInfo")
            if isinstance(result, list):
                peers = []
                for p in result:
                    gs     = p.get("graphstate") or {}
                    addr   = p.get("address", p.get("addr", ""))
                    # strip the p2p suffix for display: /ip4/1.2.3.4/tcp/8150/p2p/16U...
                    parts  = addr.split("/")
                    ip_port = "/".join(parts[:5]) if len(parts) >= 5 else addr
                    peers.append({
                        "addr":      ip_port,
                        "full_addr": addr,
                        "inbound":   p.get("direction", "Outbound").lower() == "inbound",
                        "conntime":  p.get("conntime", ""),
                        "version":   p.get("version", ""),
                        "height":    gs.get("mainheight"),
                        "bytes_rx":  p.get("bytesrecv", 0),
                        "bytes_tx":  p.get("bytessent", 0),
                    })
                self._json({"peers": peers, "count": len(peers)})
            else:
                self._json({"peers": [], "count": 0, "note": "getPeerInfo returned no data"})
        except Exception as e:
            self._json({"peers": [], "count": 0, "error": str(e)})

    def _get_haproxy_status(self):
        try:
            # Use the internal stats HTTP endpoint (port 9999, not exposed to host).
            # busybox wget is always present in haproxy:2.9-alpine; no socat needed.
            r = subprocess.run(
                ["docker", "exec", "rpc-failover",
                 "wget", "-qO-", "http://127.0.0.1:9999/;csv"],
                capture_output=True, text=True, timeout=8)
            output = r.stdout.strip()
            if not output or r.returncode != 0:
                self._json({"backends": {}, "active": None, "fallback": True})
                return
            backends = {}
            active_node = None
            for line in output.splitlines():
                if line.startswith("#") or not line.strip():
                    continue
                parts = line.split(",")
                if len(parts) < 20:
                    continue
                svname  = parts[1]
                status  = parts[17]
                is_act  = parts[19]   # "1" if server is active (not backup)
                if svname in ("FRONTEND", "BACKEND"):
                    continue
                backends[svname] = status
                # First active (non-backup) UP server = the node currently routing traffic
                if status == "UP" and is_act == "1" and active_node is None:
                    active_node = svname
            # Fallback: if no active server found, take any UP server (e.g. backup in use)
            if active_node is None:
                active_node = next((k for k, v in backends.items() if v == "UP"), None)
            self._json({"backends": backends, "active": active_node, "fallback": False})
        except Exception as e:
            self._json({"backends": {}, "active": None, "fallback": True, "error": str(e)})

    def _verify_backup(self):
        try:
            backup_path = Path(_get_backup_dir())
            dirs = sorted(
                [d for d in backup_path.iterdir()
                 if d.is_dir() and d.name.startswith("blockdag-backup-")],
                key=lambda d: d.stat().st_mtime)
            if not dirs:
                self._json({"ok": False, "error": "No backup directories found"})
                return
            latest = dirs[-1]

            def count_files(path):
                try:
                    return sum(1 for f in Path(path).rglob("*") if f.is_file())
                except Exception:
                    return 0

            results = {}
            all_ok = True
            for node, src in [("node1", NODE1_DATA), ("node2", NODE2_DATA)]:
                src_count    = count_files(src)
                backup_count = count_files(latest / node)
                diff         = abs(src_count - backup_count)
                ok           = diff <= max(5, int(src_count * 0.01))
                results[node] = {"ok": ok, "src_files": src_count, "backup_files": backup_count}
                if not ok:
                    all_ok = False

            self._json({"ok": all_ok, "backup": latest.name, "nodes": results})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_env_config(self):
        """Return current values of exposed env keys from the main .env file."""
        data = _parse_env(ENV_FILE)
        self._json({k: data.get(k, "") for k in ENV_EXPOSED_KEYS})

    def _save_env_config(self):
        """Write posted key/value pairs to both .env files, rebuild PG_URL,
        update Postgres credentials if they changed, then restart the stack."""
        try:
            body = json.loads(self._body())

            # Snapshot old Postgres credentials BEFORE writing new values
            old_data    = _parse_env(ENV_FILE)
            old_pg_user = old_data.get("POSTGRES_USER", "test")
            old_pg_pass = old_data.get("POSTGRES_PASSWORD", "test")

            for key in ENV_EXPOSED_KEYS:
                if key in body and key != "PG_URL":   # PG_URL is auto-rebuilt below
                    _write_env_key(ENV_FILE, key, body[key])
                    _write_env_key(ENV_POOL_FILE, key, body[key])

            # Rebuild PG_URL from new postgres fields
            data        = _parse_env(ENV_FILE)
            new_pg_user = data.get("POSTGRES_USER", "test")
            new_pg_pass = data.get("POSTGRES_PASSWORD", "test")
            pg_url = (
                f"postgres://{new_pg_user}"
                f":{new_pg_pass}"
                f"@pool-db:5432/{data.get('POSTGRES_DB','pool')}"
            )
            _write_env_key(ENV_FILE, "PG_URL", pg_url)
            _write_env_key(ENV_POOL_FILE, "PG_URL", pg_url)

            pg_user_changed = new_pg_user != old_pg_user
            pg_pass_changed = new_pg_pass != old_pg_pass

            # Restart the stack in background so response returns immediately
            def _restart():
                try:
                    # Apply Postgres credential changes while the DB is still running
                    if pg_user_changed or pg_pass_changed:
                        safe_pass = new_pg_pass.replace("'", "''")
                        if pg_user_changed:
                            sql = (
                                f'CREATE USER "{new_pg_user}" WITH SUPERUSER '
                                f"PASSWORD '{safe_pass}'; "
                                f'ALTER DATABASE pool OWNER TO "{new_pg_user}";'
                            )
                            print(f"[Config] Creating Postgres user '{new_pg_user}'...")
                        else:
                            sql = f'ALTER USER "{old_pg_user}" WITH PASSWORD \'{safe_pass}\';'
                            print(f"[Config] Updating Postgres password for '{old_pg_user}'...")
                        r = subprocess.run(
                            ["docker", "compose", "exec", "-T", "pool-db",
                             "psql", "-U", old_pg_user, "-d", "pool", "-c", sql],
                            cwd=str(INSTALL_DIR), capture_output=True, text=True)
                        if r.returncode != 0 and "already exists" not in r.stderr:
                            print(f"[Config] Warning: Postgres credential update failed: {r.stderr.strip()}")
                        else:
                            print("[Config] Postgres credentials updated successfully.")
                    subprocess.run(
                        ["docker", "compose", "down"],
                        cwd=str(INSTALL_DIR), capture_output=True)
                    subprocess.run(
                        ["docker", "compose", "up", "-d"],
                        cwd=str(INSTALL_DIR), capture_output=True)
                    print("[Config] Stack restarted after config change.")
                except Exception as ex:
                    print(f"[Config] Restart failed: {ex}")
            threading.Thread(target=_restart, daemon=True).start()
            self._json({"ok": True})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_maintenance_config(self):
        try:
            if MAINTENANCE_FILE.exists():
                cfg = json.loads(MAINTENANCE_FILE.read_text())
            else:
                cfg = {}
            self._json({
                "enabled":    cfg.get("enabled", False),
                "time":       cfg.get("time", "03:00"),
                "days":       cfg.get("days", ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]),
                "containers": cfg.get("containers", ["bdag-miner-node-1","bdag-miner-node-2","asic-pool"]),
            })
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _set_maintenance_config(self):
        try:
            body = json.loads(self._body())
            enabled    = bool(body.get("enabled", False))
            mtime      = str(body.get("time", "03:00"))
            days       = body.get("days", ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"])
            containers = body.get("containers", ["bdag-miner-node-1","bdag-miner-node-2","asic-pool"])

            cfg = {"enabled": enabled, "time": mtime, "days": days, "containers": containers}
            MAINTENANCE_FILE.write_text(json.dumps(cfg, indent=2))

            # Write restart script
            restart_script = Path(__file__).parent / "blockdag-maintenance-restart.ps1"
            lines = ["# BlockDAG Maintenance Restart Script (auto-generated)", ""]
            for c in containers:
                lines.append(f'docker restart {c}')
            restart_script.write_text("\n".join(lines), encoding="utf-8")

            day_map = {
                "Mon": "Monday", "Tue": "Tuesday", "Wed": "Wednesday",
                "Thu": "Thursday", "Fri": "Friday", "Sat": "Saturday", "Sun": "Sunday"
            }
            ps_days = ",".join(day_map.get(d, d) for d in days)
            script_path = str(restart_script)

            if enabled:
                ps = (
                    f'$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek {ps_days} -At "{mtime}";'
                    f'$action  = New-ScheduledTaskAction -Execute "powershell.exe"'
                    f' -Argument \'-NonInteractive -File "{script_path}"\';'
                    f'$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 1);'
                    f'$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -RunLevel Highest;'
                    f'Unregister-ScheduledTask -TaskName "BlockDAG-Maintenance" -Confirm:$false -ErrorAction SilentlyContinue;'
                    f'Register-ScheduledTask -TaskName "BlockDAG-Maintenance" -Trigger $trigger'
                    f' -Action $action -Settings $settings -Principal $principal | Out-Null;'
                    f'Write-Output "OK"'
                )
            else:
                ps = (
                    'Unregister-ScheduledTask -TaskName "BlockDAG-Maintenance"'
                    ' -Confirm:$false -ErrorAction SilentlyContinue;'
                    'Write-Output "OK"'
                )

            out, err, rc = self._ps(ps)
            if rc == 0:
                self._json({"ok": True})
            else:
                self._json({"ok": False, "error": err or out}, 500)
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_watchdog_config(self):
        try:
            cfg = _load_watchdog_cfg()
            self._json({
                "node_watchdog_enabled": cfg.get("node_watchdog_enabled", True),
                "node_grace":            cfg.get("node_grace",   WATCHDOG_GRACE),
                "node_startup":          cfg.get("node_startup", WATCHDOG_STARTUP),
                "pool_watchdog_enabled": cfg.get("pool_watchdog_enabled", True),
                "pool_grace":            cfg.get("pool_grace",   POOL_WATCHDOG_GRACE),
                "pool_startup":          cfg.get("pool_startup", POOL_WATCHDOG_STARTUP),
            })
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _save_watchdog_config(self):
        try:
            body = json.loads(self._body())
            cfg = {
                "node_watchdog_enabled": bool(body.get("node_watchdog_enabled", True)),
                "node_grace":   max(30, int(body.get("node_grace",   WATCHDOG_GRACE))),
                "node_startup": max(0,  int(body.get("node_startup", WATCHDOG_STARTUP))),
                "pool_watchdog_enabled": bool(body.get("pool_watchdog_enabled", True)),
                "pool_grace":   max(30, int(body.get("pool_grace",   POOL_WATCHDOG_GRACE))),
                "pool_startup": max(0,  int(body.get("pool_startup", POOL_WATCHDOG_STARTUP))),
            }
            _save_watchdog_cfg(cfg)
            self._json({"ok": True})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _get_autostart_config(self):
        """Return whether the HKCU Run autostart entry exists."""
        if not _winreg:
            self._json({"enabled": False, "error": "winreg not available"}); return
        try:
            enabled = False
            with _winreg.OpenKey(_winreg.HKEY_CURRENT_USER, AUTOSTART_REG_KEY) as k:
                try:
                    _winreg.QueryValueEx(k, AUTOSTART_REG_NAME)
                    enabled = True
                except FileNotFoundError:
                    pass
            self._json({"enabled": enabled})
        except Exception as e:
            self._json({"enabled": False, "error": str(e)})

    def _save_autostart_config(self):
        """Add or remove a HKCU\\Run registry entry that starts the stack at login.

        Uses winreg directly — no PowerShell, no Task Scheduler, no elevation needed.
        Start-Sleep 60 inside the command gives Docker Desktop time to initialise.
        """
        if not _winreg:
            self._json({"ok": False, "error": "winreg not available (non-Windows?)"}); return
        try:
            body    = json.loads(self._body())
            enabled = bool(body.get("enabled", False))
            with _winreg.OpenKey(
                    _winreg.HKEY_CURRENT_USER, AUTOSTART_REG_KEY,
                    0, _winreg.KEY_SET_VALUE) as k:
                if enabled:
                    install_str = str(INSTALL_DIR)
                    run_val = (
                        f"powershell -WindowStyle Hidden -NonInteractive "
                        f"-ExecutionPolicy Bypass "
                        f"-Command \"Start-Sleep 60; "
                        f"Set-Location '{install_str}'; docker compose up -d\""
                    )
                    _winreg.SetValueEx(k, AUTOSTART_REG_NAME, 0, _winreg.REG_SZ, run_val)
                else:
                    try:
                        _winreg.DeleteValue(k, AUTOSTART_REG_NAME)
                    except FileNotFoundError:
                        pass  # already gone — not an error
            self._json({"ok": True, "enabled": enabled})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)


def _watchdog_check(container):
    """Check one container for sync freeze. Restart it if stuck too long."""
    _cfg    = _load_watchdog_cfg()
    if not _cfg.get("node_watchdog_enabled", True):
        return
    grace   = _cfg.get("node_grace",   WATCHDOG_GRACE)
    startup = _cfg.get("node_startup", WATCHDOG_STARTUP)

    now = time.time()

    # Skip if container restarted too recently (let it finish startup)
    try:
        r = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.StartedAt}}", container],
            capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            started_str = r.stdout.strip().split(".")[0] + "Z"
            started = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
            uptime_secs = (datetime.now(timezone.utc) - started).total_seconds()
            if uptime_secs < startup:
                return
    except Exception:
        return

    # Pull last 5 minutes of logs
    try:
        r = subprocess.run(
            ["docker", "logs", "--since", "5m", container],
            capture_output=True, text=True, timeout=15)
        logs = re.sub(r'\x1b\[[0-9;]*m|\[0m', '', r.stdout + r.stderr)
    except Exception:
        return

    has_imported = bool(re.search(r'Imported new chain segment', logs))
    cur_matches  = re.findall(r'cur=\((\d+(?:,\d+)*)\)', logs)

    with _watchdog_lock:
        state = _watchdog_state.setdefault(container, {"frozen_since": None})

        if has_imported:
            # Node is actively importing blocks — healthy
            state["frozen_since"] = None
            return

        if not cur_matches:
            # No sync activity — caught up or still starting up
            state["frozen_since"] = None
            return

        # Syncing but no imports — check if cur= is stuck at a single value
        if len(set(cur_matches)) > 1:
            # Multiple different cur= values → some DAG progress happening
            state["frozen_since"] = None
            return

        # All sync attempts in last 5 min have the same cur= and zero imports → frozen
        if state["frozen_since"] is None:
            state["frozen_since"] = now
            print(f"[watchdog] {container}: freeze detected "
                  f"(cur={cur_matches[-1]}), will restart in {grace}s if still stuck")
            return

        frozen_secs = now - state["frozen_since"]
        if frozen_secs < grace:
            print(f"[watchdog] {container}: still frozen ({frozen_secs:.0f}s / {grace}s grace)")
            return

        # Grace period expired — restart
        print(f"[watchdog] {container}: frozen {frozen_secs:.0f}s — auto-restarting")
        try:
            subprocess.run(["docker", "restart", container], timeout=60)
            print(f"[watchdog] {container}: restarted OK — peers will reconnect via --addpeer")
        except Exception as e:
            print(f"[watchdog] {container}: restart failed: {e}")
        state["frozen_since"] = None


def _watchdog_loop():
    time.sleep(90)  # stagger 90s after startup so alert_loop gets a head start
    while True:
        try:
            for container in WATCHDOG_CONTAINERS:
                _watchdog_check(container)
        except Exception as e:
            print(f"[watchdog] loop error: {e}")
        time.sleep(WATCHDOG_INTERVAL)


def _pool_nonce_watchdog_check():
    """Restart asic-pool if nonce-too-low persists beyond POOL_WATCHDOG_GRACE seconds.

    The error pattern is: nonce too low: address 0x..., tx: N state: M
    It appears when the payout wallet's on-chain nonce diverges from what the pool
    tracks in memory (typically after a large burst of payout transactions).
    The only fix is a pool restart, which re-reads the on-chain nonce from scratch.
    """
    _cfg    = _load_watchdog_cfg()
    if not _cfg.get("pool_watchdog_enabled", True):
        with _pool_watchdog_lock:
            _pool_watchdog_state["nonce_error_since"] = None
        return
    grace   = _cfg.get("pool_grace",   POOL_WATCHDOG_GRACE)
    startup = _cfg.get("pool_startup", POOL_WATCHDOG_STARTUP)

    now = time.time()

    # Skip if pool restarted too recently (let it finish startup / nonce resync)
    try:
        r = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.StartedAt}}", "asic-pool"],
            capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            started_str = r.stdout.strip().split(".")[0] + "Z"
            started = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
            uptime_secs = (datetime.now(timezone.utc) - started).total_seconds()
            if uptime_secs < startup:
                return
    except Exception:
        return

    # Pull last 3 minutes of pool logs
    try:
        r = subprocess.run(
            ["docker", "logs", "--since", "3m", "asic-pool"],
            capture_output=True, text=True, timeout=15)
        logs = re.sub(r'\x1b\[[0-9;]*m|\[0m', '', r.stdout + r.stderr)
    except Exception:
        return

    has_nonce_error = bool(re.search(r'nonce too low', logs, re.IGNORECASE))

    with _pool_watchdog_lock:
        state = _pool_watchdog_state

        if not has_nonce_error:
            if state["nonce_error_since"] is not None:
                state["nonce_error_since"] = None
                print("[pool-watchdog] asic-pool: nonce error cleared — healthy")
            return

        # Nonce error detected — start or continue grace timer
        if state["nonce_error_since"] is None:
            state["nonce_error_since"] = now
            print(f"[pool-watchdog] asic-pool: nonce-too-low detected, "
                  f"will restart in {grace}s if still present")
            return

        error_secs = now - state["nonce_error_since"]
        if error_secs < grace:
            print(f"[pool-watchdog] asic-pool: nonce error persisting "
                  f"({error_secs:.0f}s / {grace}s grace)")
            return

        # Grace period expired — restart the pool
        print(f"[pool-watchdog] asic-pool: nonce error for {error_secs:.0f}s — auto-restarting")
        try:
            subprocess.run(["docker", "restart", "asic-pool"], timeout=60)
            print("[pool-watchdog] asic-pool: restarted OK")
        except Exception as e:
            print(f"[pool-watchdog] asic-pool: restart failed: {e}")
        state["nonce_error_since"] = None


def _pool_watchdog_loop():
    time.sleep(30)  # stagger — start checking shortly after dashboard is up
    while True:
        try:
            _pool_nonce_watchdog_check()
        except Exception as e:
            print(f"[pool-watchdog] loop error: {e}")
        time.sleep(POOL_WATCHDOG_INTERVAL)


if __name__ == "__main__":
    threading.Thread(target=_alert_loop,        daemon=True).start()
    threading.Thread(target=_watchdog_loop,     daemon=True).start()
    threading.Thread(target=_pool_watchdog_loop, daemon=True).start()
    print(f"BlockDAG Dashboard  ->  http://localhost:{PORT}")
    print("Press Ctrl+C to stop.\n")
    ThreadingHTTPServer(("", PORT), Handler).serve_forever()
