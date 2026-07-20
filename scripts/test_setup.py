"""Validate environment, API keys, and TikTok/OpenAI connectivity before scaling.

Usage (from project root):
    python scripts/test_setup.py
    python scripts/test_setup.py --config path/to/config.yaml

Does not print or save secrets. Sample output is written to data/test_setup_validation.json
(typically gitignored via data/).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openai import APIStatusError, OpenAI

from tiktok import auth
from tiktok.config import load_config
from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)

OUTPUT_REL = os.path.join("data", "test_setup_validation.json")


def describe_key_presence(value: Optional[str]) -> str:
    """Report whether a key is set and its length — no substring of the secret."""
    if not value or not str(value).strip():
        return "missing or empty"
    return f"present ({len(str(value).strip())} chars)"


def log_http_failure(label: str, resp: requests.Response) -> None:
    """Log HTTP errors with status; truncate body to avoid huge dumps."""
    text = (resp.text or "")[:800]
    logger.error("%s — HTTP %s %s", label, resp.status_code, resp.reason)
    if text:
        logger.error("Response body (truncated): %s", text)


def try_load_config(config_path: str) -> tuple[Optional[Any], Optional[str]]:
    """Return (config, None) or (None, error message)."""
    try:
        cfg = load_config(config_path)
        return cfg, None
    except KeyError as e:
        return None, f"Missing environment variable: {e}"
    except FileNotFoundError as e:
        return None, str(e)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def test_openai(api_key: str) -> Dict[str, Any]:
    """Minimal OpenAI call: list models (no completion cost)."""
    out: Dict[str, Any] = {"ok": False}
    try:
        client = OpenAI(api_key=api_key)
        listed = client.models.list()
        first_id = listed.data[0].id if listed.data else None
        out["ok"] = True
        out["sample_model_id"] = first_id
        out["model_count"] = len(listed.data)
        logger.info("OpenAI: models.list() OK (e.g. first id=%s)", first_id)
    except APIStatusError as e:
        out["error"] = "api_status_error"
        out["http_status"] = e.status_code
        logger.error(
            "OpenAI API error — HTTP %s: %s",
            e.status_code,
            str(e)[:500],
        )
    except Exception as e:
        out["error"] = type(e).__name__
        out["detail"] = str(e)[:500]
        logger.error("OpenAI request failed: %s: %s", type(e).__name__, e)
    return out


def test_tiktok_oauth_and_user_info(cfg: Any, usernames: List[str]) -> Dict[str, Any]:
    """OAuth token + one or two research user/info calls; return structured result."""
    out: Dict[str, Any] = {
        "oauth": {"ok": False},
        "user_info": [],
    }
    auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)

    try:
        token = auth.get_access_token()
        if token:
            out["oauth"] = {"ok": True, "token_length": len(token)}
            logger.info("TikTok OAuth: access token obtained (%s chars)", len(token))
    except requests.HTTPError as e:
        resp = e.response
        status = resp.status_code if resp is not None else None
        out["oauth"] = {"ok": False, "http_status": status}
        if resp is not None:
            log_http_failure("TikTok OAuth token", resp)
        else:
            logger.error("TikTok OAuth HTTPError without response: %s", e)
        return out
    except Exception as e:
        out["oauth"] = {"ok": False, "error": type(e).__name__, "detail": str(e)[:500]}
        logger.error("TikTok OAuth failed: %s: %s", type(e).__name__, e)
        return out

    from tiktok.auth import auth_headers

    endpoint = "research/user/info/"
    url = f"{cfg.base_url.rstrip('/')}/{endpoint}"
    fields = "display_name,follower_count,video_count,is_verified"

    for username in usernames:
        row: Dict[str, Any] = {"username": username}
        try:
            resp = requests.post(
                url,
                headers={**auth_headers(), "Content-Type": "application/json"},
                json={"username": username},
                params={"fields": fields},
                timeout=30,
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            row["ok"] = False
            row["error"] = type(e).__name__
            logger.error("TikTok user info request failed for @%s: %s", username, e)
            out["user_info"].append(row)
            continue

        row["http_status"] = resp.status_code
        if resp.status_code == 429:
            logger.error(
                "TikTok user info — HTTP 429 Too Many Requests for @%s (rate limit)",
                username,
            )
            retry_after = resp.headers.get("Retry-After", "?")
            logger.error("Retry-After: %s", retry_after)
            try:
                row["response_hint"] = resp.json()
            except ValueError:
                row["response_hint"] = (resp.text or "")[:400]
            out["user_info"].append(row)
            continue

        if not resp.ok:
            log_http_failure(f"TikTok user info @{username}", resp)
            try:
                row["body"] = resp.json()
            except ValueError:
                row["body"] = {"raw_text": (resp.text or "")[:400]}
            out["user_info"].append(row)
            continue

        try:
            body = resp.json()
        except ValueError:
            row["ok"] = False
            row["parse_error"] = "invalid json"
            out["user_info"].append(row)
            continue

        data = body.get("data") or {}
        row["ok"] = True
        row["data"] = {
            "display_name": data.get("display_name"),
            "follower_count": data.get("follower_count"),
            "video_count": data.get("video_count"),
            "is_verified": data.get("is_verified"),
        }
        logger.info("TikTok user info OK for @%s", username)
        out["user_info"].append(row)

    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate API setup (no secrets printed)")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    args = parser.parse_args()

    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    report: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "load_config": {"ok": False},
        "env_keys_status": {},
        "openai": {},
        "tiktok": {},
    }

    cfg, err = try_load_config(args.config)
    if cfg is None:
        report["load_config"]["error"] = err
        logger.error("load_config() failed: %s", err)
        _write_report(report)
        return 1

    report["load_config"] = {"ok": True, "config_path": args.config}
    report["env_keys_status"] = {
        "TIKTOK_CLIENT_KEY": describe_key_presence(cfg.tiktok_client_key),
        "TIKTOK_CLIENT_SECRET": describe_key_presence(cfg.tiktok_client_secret),
        "OPENAI_API_KEY": describe_key_presence(cfg.openai_api_key),
    }

    logger.info("Environment keys: %s", report["env_keys_status"])

    # OpenAI
    report["openai"] = test_openai(cfg.openai_api_key)

    # TikTok: OAuth + up to two user/info rows from "sample"
    try:
        handles = cfg.get_handles("sample")[:2]
    except ValueError as e:
        logger.error("Cannot resolve handle group: %s", e)
        handles = []

    if not handles:
        logger.error(
            "No handles in 'sample' group — add handles under handle_groups.sample "
            "in config.yaml for user/info validation."
        )

    report["tiktok"] = test_tiktok_oauth_and_user_info(cfg, handles)

    _write_report(report)

    if _all_checks_passed(report):
        logger.info("Overall: SUCCESS (see %s)", OUTPUT_REL)
        return 0

    logger.error("Overall: FAILURE or partial failure (see %s)", OUTPUT_REL)
    return 1


def _all_checks_passed(report: Dict[str, Any]) -> bool:
    if not report.get("load_config", {}).get("ok"):
        return False
    if not report.get("openai", {}).get("ok"):
        return False
    tik = report.get("tiktok") or {}
    if not tik.get("oauth", {}).get("ok"):
        return False
    rows = tik.get("user_info") or []
    if not rows:
        return False
    return all(r.get("ok") is True for r in rows)


def _write_report(report: Dict[str, Any]) -> None:
    out_path = os.path.join(os.getcwd(), OUTPUT_REL)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Wrote report to %s", out_path)


if __name__ == "__main__":
    sys.exit(main())
