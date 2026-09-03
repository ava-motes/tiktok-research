"""Validate TikTok handles via Research API user/info (no enrichment).

Writes a CSV with handle, status (valid/invalid), and error_message.
Does NOT remove invalid handles from config.yaml.

Usage (on comm-cme-p01 from project root):
    python scripts/validate_handles.py --group complete
    python scripts/validate_handles.py --group sample --limit 10
    python scripts/validate_handles.py --group complete \\
        --output data/exports/handle_validation.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from pathlib import Path
import importlib.util

def _setup_repo():
    for p in Path(__file__).resolve().parents:
        boot = p / "common" / "bootstrap.py"
        if boot.is_file():
            spec = importlib.util.spec_from_file_location("_tiktok_bootstrap", boot)
            mod = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(mod)
            return mod.setup()
    raise RuntimeError("common/bootstrap.py not found")

ROOT = _setup_repo()

from tiktok import auth
from tiktok.config import load_config
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)

USER_FIELDS = (
    "display_name,bio_description,is_verified,"
    "follower_count,following_count,likes_count,video_count"
)


def _normalize_handle(raw: str) -> str:
    return raw.strip().lstrip("@")


def _error_message(resp: Optional[requests.Response], exc: Optional[BaseException] = None) -> str:
    if exc is not None:
        return f"{type(exc).__name__}: {exc}"[:500]
    if resp is None:
        return "no_response"
    try:
        body = resp.json()
    except ValueError:
        return f"HTTP {resp.status_code}: {(resp.text or '')[:300]}"
    err = body.get("error") if isinstance(body, dict) else None
    if isinstance(err, dict):
        code = err.get("code") or err.get("message") or ""
        msg = err.get("message") or err.get("log_id") or ""
        detail = f"{code}: {msg}".strip(": ")
        return f"HTTP {resp.status_code}: {detail}"[:500]
    return f"HTTP {resp.status_code}: {(resp.text or '')[:300]}"


def validate_handle(base_url: str, handle: str, max_retries: int = 5) -> Dict[str, str]:
    """Call research/user/info for one handle; return status row."""
    url = f"{base_url.rstrip('/')}/research/user/info/"
    body = {"username": handle}
    params = {"fields": USER_FIELDS}

    for attempt in range(max_retries):
        try:
            resp = requests.post(
                url,
                headers={**auth.auth_headers(), "Content-Type": "application/json"},
                json=body,
                params=params,
                timeout=30,
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt + 1 < max_retries:
                time.sleep(2 ** attempt)
                continue
            return {
                "handle": handle,
                "status": "invalid",
                "error_message": _error_message(None, e),
            }

        if resp.status_code == 429:
            try:
                code = resp.json().get("error", {}).get("code", "")
            except ValueError:
                code = ""
            if code == "daily_quota_limit_exceeded":
                return {
                    "handle": handle,
                    "status": "invalid",
                    "error_message": "daily_quota_limit_exceeded",
                }
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning("@%s rate limited — waiting %ss", handle, retry_after)
            time.sleep(retry_after)
            continue

        if not resp.ok:
            return {
                "handle": handle,
                "status": "invalid",
                "error_message": _error_message(resp),
            }

        try:
            payload = resp.json()
        except ValueError:
            return {
                "handle": handle,
                "status": "invalid",
                "error_message": _error_message(resp),
            }

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict) or not data:
            err = _error_message(resp)
            # Some "ok" responses still signal missing users in error block
            return {
                "handle": handle,
                "status": "invalid",
                "error_message": err if "HTTP" in err else "empty_user_data",
            }

        return {"handle": handle, "status": "valid", "error_message": ""}

    return {
        "handle": handle,
        "status": "invalid",
        "error_message": "exhausted_retries",
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate handle group against TikTok Research API user/info"
    )
    parser.add_argument("--group", default="complete", help="Handle group from config.yaml")
    parser.add_argument("--config", default="common/config.yaml", help="Path to config file")
    parser.add_argument("--limit", type=int, default=None, help="Validate only first N handles")
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default: data/exports/handle_validation_<ts>.csv)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.25,
        help="Delay between requests in seconds (default 0.25)",
    )
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)

    handles = [_normalize_handle(h) for h in cfg.get_handles(args.group)]
    handles = [h for h in handles if h]
    if args.limit is not None:
        handles = handles[: args.limit]

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = args.output or os.path.join(
        cfg.paths.get("exports", "data/exports"),
        f"handle_validation_{args.group}_{ts}.csv",
    )
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    logger.info(
        "Validating %s handles from group '%s' → %s",
        len(handles),
        args.group,
        out_path,
    )

    rows: List[Dict[str, str]] = []
    valid = invalid = 0
    for i, handle in enumerate(handles, 1):
        row = validate_handle(cfg.base_url, handle)
        rows.append(row)
        if row["status"] == "valid":
            valid += 1
            logger.info("[%s/%s] @%s valid", i, len(handles), handle)
        else:
            invalid += 1
            logger.warning(
                "[%s/%s] @%s invalid — %s",
                i,
                len(handles),
                handle,
                row["error_message"],
            )
        if args.sleep > 0 and i < len(handles):
            time.sleep(args.sleep)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["handle", "status", "error_message"])
        writer.writeheader()
        writer.writerows(rows)

    print(
        f"total={len(rows)} valid={valid} invalid={invalid} output={out_path}",
        flush=True,
    )
    if invalid:
        print("Invalid handles:", flush=True)
        for row in rows:
            if row["status"] != "valid":
                print(f"  @{row['handle']}: {row['error_message']}", flush=True)

    # Never rewrite config; exit 0 even with invalids so collection can still be planned.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
