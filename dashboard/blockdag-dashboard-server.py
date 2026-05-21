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

import json, os, shutil, subprocess, urllib.request, re, threading, time, base64
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from datetime import datetime, timezone, timedelta
from pathlib import Path

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
]

CONTAINERS  = ["bdag-miner-node-1", "bdag-miner-node-2",
               "asic-pool", "rpc-failover", "pool-db"]
DASH_FILE        = Path(__file__).parent / "blockdag-dashboard.html"
ALERTS_FILE      = Path(__file__).parent / "alerts.json"
PAYOUT_FILE      = Path(__file__).parent / "payout.json"
BACKUP_CFG_FILE  = Path(__file__).parent / "backup-config.json"
ALERT_HISTORY_FILE = Path(__file__).parent / "alert-history.json"
MAINTENANCE_FILE = Path(__file__).parent / "maintenance.json"
PEERS_FILE         = Path(__file__).parent / "Network Peers.txt"
PEERS_MANAGED_FILE = Path(__file__).parent / "peers-managed.txt"
COMPOSE_FILE       = INSTALL_DIR / "docker-compose.yml"
MAX_ALERT_HISTORY  = 50
ALERT_COOLDOWN = 900  # seconds between repeat alerts for same condition

_alert_lock  = threading.Lock()
_alert_state = {
    "freeze":     {"active": False, "last_sent": 0.0},
    "peers":      {"active": False, "last_sent": 0.0},
    "divergence": {"active": False, "last_sent": 0.0},
    "disk":       {"active": False, "last_sent": 0.0},
}
_last_auto_add_time = None   # ISO string, updated when auto-add fires
_node_id_cache      = {}     # container_name -> node_id string (cached from startup log)
_node_version_cache = {}     # container_name -> version string (cached from startup log)

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
        elif self.path == "/poolstats":
            self._get_poolstats()
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
        elif self.path == "/maintenance/config":
            self._get_maintenance_config()
        elif self.path == "/env/config":
            self._get_env_config()
        elif self.path.startswith("/logs"):
            self._get_logs()
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
        elif p == "/setup/tasks":      self._run_setup_tasks()
        elif p == "/peers/add":        self._add_peers()
        elif p == "/peers/clear":      self._clear_peers()
        elif p == "/peers/save":       self._save_peers()
        elif p == "/maintenance/config": self._set_maintenance_config()
        elif p == "/env/config":       self._save_env_config()
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
        url = self._evm_url()
        if not url:
            self._json({"error": "Could not get container IP"}, 502)
            return
        body = self._body()
        req  = urllib.request.Request(url, data=body,
                                      headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                data = r.read()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._cors(); self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self._json({"error": str(e)}, 502)

    def _get_poolstats(self):
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
                    "SELECT address, amount, to_char(created_at,'MM-DD HH24:MI') "
                    "FROM payouts ORDER BY created_at DESC LIMIT 20")
                for row in ph:
                    if len(row) >= 3:
                        addr = str(row[0])
                        payout_history.append({
                            "address": addr,
                            "bdag":    round(int(row[1]) / 1e18, 8),
                            "time":    row[2],
                        })
            except Exception:
                pass

            # ── Active miners (seen in last 24 h) ─────────────────────────────
            am = psql("SELECT COUNT(*) FROM miners "
                      "WHERE last_active > NOW() - INTERVAL '24 hours'")
            active_miners = int(am[0][0]) if am else 0

            # ── 5 most recent blocks ──────────────────────────────────────────
            rb = psql(
                "SELECT hash, height, status, "
                "to_char(created_at,'MM-DD HH24:MI') "
                "FROM blocks ORDER BY created_at DESC LIMIT 5")
            recent = [{"hash": r[0][:14] + "…", "height": int(r[1]),
                       "status": r[2], "time": r[3]}
                      for r in rb if len(r) >= 4]

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

            # Blocks found today
            blocks_today = 0
            try:
                bt = psql("SELECT COUNT(*) FROM blocks "
                          "WHERE created_at > NOW() - INTERVAL '24 hours'")
                if bt:
                    blocks_today = int(bt[0][0])
            except Exception:
                pass

            # Round duration: seconds since last block
            round_secs = None
            try:
                rd = psql(
                    "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))::int FROM blocks")
                if rd and rd[0][0]:
                    round_secs = int(float(rd[0][0]))
            except Exception:
                pass

            # Hashrate estimate from share difficulty (last 10 min)
            hashrate_mhs = None
            try:
                hr = psql(
                    "SELECT COALESCE(SUM(difficulty),0) FROM shares "
                    "WHERE created_at > NOW() - INTERVAL '10 minutes' AND is_valid = TRUE")
                if hr and float(hr[0][0]) > 0:
                    hashrate_mhs = round(float(hr[0][0]) / 600 / 1e6, 2)
            except Exception:
                pass

            # Per-miner stats from miners table (shares are tracked in-memory by the pool,
            # not persisted to the DB, so share counts and hashrate are unavailable here)
            miners_list = []
            worker_map = _dagtech_worker_map()
            try:
                # Try to include the IP column; fall back if the column doesn't exist.
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
                hashrate_by_addr = {}
                try:
                    hr_rows = psql(
                        "SELECT address, COALESCE(SUM(difficulty),0) "
                        "FROM shares "
                        "WHERE created_at > NOW() - INTERVAL '10 minutes' AND is_valid = TRUE "
                        "GROUP BY address")
                    for hr in hr_rows:
                        if len(hr) >= 2 and float(hr[1]) > 0:
                            hashrate_by_addr[str(hr[0]).lower()] = round(float(hr[1]) / 600 / 1e6, 4)
                except Exception:
                    pass

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
                            "last_active":  str(row[1])[:16],
                            "accepted":     0,
                            "rejected":     0,
                            "hashrate_mhs": hashrate_by_addr.get(addr.lower(), 0),
                        })
            except Exception:
                pass

            self._json({
                "total_blocks":   total_blocks,
                "blocks":         blocks,
                "total_reward_bdag": round(total_reward / 1e18, 4),
                "pending_credits":   {"count": pending_count, "bdag": pending_bdag},
                "payouts":           {"count": payout_count,  "bdag": payout_bdag},
                "active_miners":  active_miners,
                "recent_blocks":  recent,
                "health":         health,
                "shares_accepted": shares_accepted,
                "shares_rejected": shares_rejected,
                "blocks_today":    blocks_today,
                "round_secs":      round_secs,
                "hashrate_mhs":    hashrate_mhs,
                "miners":          miners_list,
                "payout_history":  payout_history,
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
            bu = shutil.disk_usage(BACKUP_DIR)
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
        try:
            req = urllib.request.Request(
                "http://127.0.0.1:8880/metrics",
                headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=3) as r:
                data = json.loads(r.read())
            worker_map = _dagtech_worker_map()
            for wallet, worker in worker_map.items():
                data["wallet_full"] = wallet
                data["worker_name"] = worker
                break
            self._json(data)
        except Exception as e:
            self._json({"error": str(e)}, 502)

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
        self._json(result)

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
                if isinstance(peers, list) and peers:
                    heights = [p.get("graphstate", {}).get("mainheight", 0) or 0 for p in peers]
                    peer_max = max(heights) if heights else None
            except Exception:
                pass
            try:
                net_info = _rpc_direct("getNetworkInfo")
                if isinstance(net_info, dict):
                    total_peers = net_info.get("totalconnected", 0)
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

        # Node ID only appears in the startup log — scan the first 5 minutes only.
        try:
            r2 = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.StartedAt}}", container],
                capture_output=True, text=True, timeout=5)
            if r2.returncode == 0:
                since_str = r2.stdout.strip().split(".")[0] + "Z"
                started = datetime.fromisoformat(since_str.replace("Z", "+00:00"))
                until_str = (started + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
                r3 = subprocess.run(
                    ["docker", "logs", "--since", since_str, "--until", until_str, container],
                    capture_output=True, text=True, timeout=10)
                logs = re.sub(r'\x1b\[[0-9;]*m|\[0m', '', r3.stdout + r3.stderr)
                for line in logs.splitlines():
                    for word in line.split():
                        if word.startswith("16Uiu2H") and len(word) > 20:
                            nid = word.strip(",:")
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
        dst = os.path.join(BACKUP_DIR, f"blockdag-backup-{ts}")
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
            self._json({
                "peers": peers,
                "count": len(peers),
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
        """Write managed peer list into docker-compose.yml as --addpeer flags.
        BlockDAG nodes don't expose a live addPeer RPC; peers are bootstrap-only
        via --addpeer startup flags. Nodes must be restarted to pick up changes."""
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

            if not COMPOSE_FILE.exists():
                self._json({"ok": False, "error": f"docker-compose.yml not found at {COMPOSE_FILE}"}, 500)
                return

            compose = COMPOSE_FILE.read_text(encoding="utf-8")

            # Build set of peers already present in the file
            existing = set(re.findall(r"--addpeer=(\S+)", compose))
            new_peers = [p for p in peers if p not in existing]

            if not new_peers:
                self._json({"ok": True, "added": 0, "total": len(peers),
                            "note": "All peers already in docker-compose.yml. Restart nodes to ensure they connect."})
                return

            # Insert new --addpeer lines after the last existing --addpeer line in each NODE_ARGS block.
            # Strategy: append new lines before the first non-addpeer flag line that follows --addpeer entries.
            addpeer_block = "\n".join(f"        --addpeer={p}" for p in new_peers)

            # Find the last --addpeer line and insert after it
            last_addpeer = max((m.end() for m in re.finditer(r"--addpeer=\S+", compose)), default=None)
            if last_addpeer is None:
                self._json({"ok": False, "error": "Could not locate --addpeer section in docker-compose.yml"}, 500)
                return

            # Insert after the end of that line
            eol = compose.find("\n", last_addpeer)
            if eol == -1:
                eol = len(compose)
            compose = compose[:eol] + "\n" + addpeer_block + compose[eol:]

            COMPOSE_FILE.write_text(compose, encoding="utf-8")
            self._json({"ok": True, "added": len(new_peers), "total": len(peers),
                        "note": f"Added {len(new_peers)} peer(s) to docker-compose.yml. Restart nodes to apply."})
        except Exception as e:
            self._json({"ok": False, "error": str(e)}, 500)

    def _clear_peers(self):
        """Remove all --addpeer lines from docker-compose.yml."""
        try:
            if not COMPOSE_FILE.exists():
                self._json({"ok": False, "error": f"docker-compose.yml not found at {COMPOSE_FILE}"}, 500)
                return
            compose = COMPOSE_FILE.read_text(encoding="utf-8")
            original_lines = compose.splitlines(keepends=True)
            filtered_lines = [l for l in original_lines if not re.search(r"--addpeer=", l)]
            removed = len(original_lines) - len(filtered_lines)
            COMPOSE_FILE.write_text("".join(filtered_lines), encoding="utf-8")
            self._json({"ok": True, "removed": removed})
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
            r = subprocess.run(
                ["docker", "exec", "rpc-failover", "sh", "-c",
                 "echo 'show stat' | socat stdio /var/run/haproxy/admin.sock 2>/dev/null"],
                capture_output=True, text=True, timeout=8)
            output = r.stdout.strip()
            if not output or r.returncode != 0:
                self._json({"backends": {}, "active": None, "fallback": True})
                return
            backends = {}
            for line in output.splitlines():
                if line.startswith("#") or not line.strip():
                    continue
                parts = line.split(",")
                if len(parts) < 18:
                    continue
                pxname  = parts[0]
                svname  = parts[1]
                status  = parts[17]
                if svname in ("FRONTEND", "BACKEND"):
                    continue
                backends[svname] = status
            active = next((k for k, v in backends.items() if v == "UP"), None)
            self._json({"backends": backends, "active": active, "fallback": False})
        except Exception as e:
            self._json({"backends": {}, "active": None, "fallback": True, "error": str(e)})

    def _verify_backup(self):
        try:
            backup_path = Path(BACKUP_DIR)
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
        """Write posted key/value pairs to both .env files, rebuild PG_URL, then restart the stack."""
        try:
            body = json.loads(self._body())
            for key in ENV_EXPOSED_KEYS:
                if key in body and key != "PG_URL":   # PG_URL is auto-rebuilt below
                    _write_env_key(ENV_FILE, key, body[key])
                    _write_env_key(ENV_POOL_FILE, key, body[key])
            # Rebuild PG_URL from current postgres fields
            data = _parse_env(ENV_FILE)
            pg_url = (
                f"postgres://{data.get('POSTGRES_USER','test')}"
                f":{data.get('POSTGRES_PASSWORD','test')}"
                f"@pool-db:5432/{data.get('POSTGRES_DB','pool')}"
            )
            _write_env_key(ENV_FILE, "PG_URL", pg_url)
            _write_env_key(ENV_POOL_FILE, "PG_URL", pg_url)
            # Restart the stack in background so response returns immediately
            def _restart():
                try:
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


if __name__ == "__main__":
    threading.Thread(target=_alert_loop, daemon=True).start()
    print(f"BlockDAG Dashboard  ->  http://localhost:{PORT}")
    print("Press Ctrl+C to stop.\n")
    ThreadingHTTPServer(("", PORT), Handler).serve_forever()
