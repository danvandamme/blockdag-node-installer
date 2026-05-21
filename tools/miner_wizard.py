#!/usr/bin/env python3
"""Scan and optionally configure BlockDAG ASICs one at a time."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops"))

from pool_ops import configure_miners, scan_miners  # noqa: E402


def yes_no(prompt: str, default: bool = False) -> bool:
    suffix = "Y/n" if default else "y/N"
    value = input(f"{prompt} [{suffix}] ").strip().lower()
    if not value:
        return default
    return value in {"y", "yes"}


def short_pool(pool: dict) -> str:
    url = str(pool.get("url", ""))
    user = str(pool.get("user", ""))
    active = " active" if pool.get("active") else ""
    return f"{url} user={user[:10]}...{user[-6:] if len(user) > 16 else user}{active}".strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scan-target", default=os.environ.get("BDAG_MINER_SCAN_TARGET", "192.168.1.0/24"))
    parser.add_argument("--pool-url", required=True)
    parser.add_argument("--worker", required=True)
    parser.add_argument("--pool-pass", default=os.environ.get("BDAG_MINER_POOL_PASSWORD", "1234"))
    parser.add_argument("--admin-password", default=os.environ.get("BDAG_MINER_ADMIN_PASSWORD", ""))
    parser.add_argument("--yes", action="store_true", help="Configure every discovered miner without asking per miner.")
    parser.add_argument("--keep-existing", action="store_true", help="Add this pool without removing old pool entries.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable scan/configuration results.")
    args = parser.parse_args()

    os.environ.setdefault("BDAG_PROJECT_ROOT", str(ROOT))
    os.environ.setdefault("BDAG_RUNTIME_DIR", str(ROOT / "ops" / "runtime"))
    os.environ.setdefault("BDAG_POOL_ENV_FILE", str(ROOT / "asic-pool" / ".env"))

    print(f"Scanning {args.scan_target} for BlockDAG ASIC web APIs...")
    scan = scan_miners(args.scan_target)
    miners = scan.get("miners") or []
    if args.json:
        print(json.dumps({"scan": scan}, indent=2))
    if not miners:
        print("No supported ASICs were found. You can rerun this later from the package directory.")
        return 0

    print(f"Found {len(miners)} supported miner(s):")
    for index, miner in enumerate(miners, 1):
        active_pool = miner.get("current_pool") or {}
        name = miner.get("model") or miner.get("hardware") or "ASIC"
        print(f"  {index}. {miner.get('ip')}  {name}  {short_pool(active_pool)}")

    configure_any = args.yes or yes_no("Configure discovered miners to this pool now?", default=False)
    if not configure_any:
        return 0

    password = args.admin_password
    if not password:
        password = getpass.getpass("ASIC admin password: ")

    all_results = []
    for miner in miners:
        ip = str(miner.get("ip"))
        if not args.yes and not yes_no(f"Configure miner {ip} now?", default=False):
            all_results.append({"ip": ip, "status": "skipped"})
            continue
        print(f"Configuring {ip}...")
        result = configure_miners(
            [ip],
            admin_password=password,
            pool_url=args.pool_url,
            worker_user=args.worker,
            pool_password=args.pool_pass,
            replace_existing=not args.keep_existing,
        )
        all_results.extend(result.get("results") or [])
        for item in result.get("results") or []:
            print(f"  {item.get('ip')}: {item.get('status')}{' - ' + item.get('error') if item.get('error') else ''}")

    if args.json:
        print(json.dumps({"results": all_results}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
