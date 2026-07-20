#!/usr/bin/env python3
"""End-to-end validation of TikTok Research pipeline (infrastructure → export).

Usage (from project root):
    python scripts/run_full_validation.py
    python scripts/run_full_validation.py --skip-openai-costs   # skip classify/transcribe
    python scripts/run_full_validation.py --quick               # infra + API only

Writes: data/full_validation_report.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import ssl
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok import auth
from tiktok.api.client import TikTokClient
from tiktok.api.comments import get_comments_for_video
from tiktok.api.download import download_audio
from tiktok.api.users import get_user_info
from tiktok.api.videos import date_chunks, query_videos_for_chunk
from tiktok.config import load_config
from tiktok.text.normalize import extract_emojis, merge_visual_text_sources
from tiktok.web.metadata import fetch_web_onscreen_text

REPORT_PATH = os.path.join("data", "full_validation_report.json")
TEST_HANDLE = "harryjsisson"
BATCH_GROUP = "batch_test"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record(results: List[Dict], name: str, ok: bool, detail: str = "", extra: Optional[Dict] = None) -> None:
    row = {"name": name, "ok": ok, "detail": detail, "at": _now()}
    if extra:
        row.update(extra)
    results.append(row)
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""))


def _run_script(args: List[str], timeout: int = 300) -> Tuple[bool, str]:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cmd = [sys.executable] + args
    try:
        proc = subprocess.run(
            cmd,
            cwd=root,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        out = (proc.stdout or "") + (proc.stderr or "")
        tail = "\n".join(out.strip().splitlines()[-8:])
        return proc.returncode == 0, tail
    except subprocess.TimeoutExpired:
        return False, f"timeout after {timeout}s"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def test_dns(results: List[Dict]) -> None:
    try:
        import dns.resolver  # optional
        answers = dns.resolver.resolve("open.tiktokapis.com", "A")
        ips = [str(r) for r in answers]
    except Exception:
        import subprocess as sp
        out = sp.check_output(["dig", "+short", "open.tiktokapis.com"], text=True)
        ips = [ln.strip() for ln in out.splitlines() if re.match(r"^\d+\.\d+\.\d+\.\d+$", ln.strip())]

    blocked = "10.159.1.22" in ips
    akamai = any(ip.startswith("23.") for ip in ips)
    _record(results, "dns_tiktok_not_blocked", not blocked and bool(ips), f"resolved: {ips[:4]}")
    _record(results, "dns_akamai_range", akamai or (not blocked and bool(ips)), f"ips={ips[:3]}")


def test_resolv_conf(results: List[Dict]) -> None:
    path = "/etc/resolv.conf"
    try:
        text = open(path).read()
    except OSError as e:
        _record(results, "dns_resolver_config", False, str(e))
        return
    uses_public = "1.1.1.1" in text or "8.8.8.8" in text
    uses_ut_only = "128.83.185" in text and not uses_public
    ok = uses_public and not uses_ut_only
    _record(results, "dns_resolver_config", ok, "public DNS" if ok else text.strip()[:120])


def test_tls(results: List[Dict]) -> None:
    ctx = ssl.create_default_context()
    try:
        with socket.create_connection(("open.tiktokapis.com", 443), timeout=15) as sock:
            with ctx.wrap_socket(sock, server_hostname="open.tiktokapis.com") as ssock:
                cert = ssock.getpeercert()
        cn = dict(x[0] for x in cert.get("subject", ()))
        common = cn.get("commonName", "")
        ok = "tiktokapis.com" in common
        _record(results, "tls_tiktok_cert", ok, f"CN={common}")
    except Exception as e:
        _record(results, "tls_tiktok_cert", False, str(e)[:200])


def test_www_tiktok(results: List[Dict]) -> None:
    import requests
    try:
        r = requests.get("https://www.tiktok.com", timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        blocked = "prohibitedtech" in (r.text or "") or "Access Denied" in (r.text or "")
        _record(results, "web_www_tiktok_com", r.status_code == 200 and not blocked, f"HTTP {r.status_code}")
    except Exception as e:
        _record(results, "web_www_tiktok_com", False, str(e)[:200])


def test_api_layer(cfg, results: List[Dict]) -> Optional[Dict[str, Any]]:
    auth.init(cfg.base_url, cfg.tiktok_client_key, cfg.tiktok_client_secret)
    client = TikTokClient(cfg.base_url, cfg.paths["raw_responses"])

    # OAuth
    try:
        token = auth.get_access_token()
        _record(results, "api_oauth", bool(token), f"token_len={len(token) if token else 0}")
    except Exception as e:
        _record(results, "api_oauth", False, str(e)[:200])
        return None

    # User info
    try:
        user = get_user_info(client, TEST_HANDLE)
        ok = bool(user and user.get("username"))
        _record(results, "api_user_info", ok, f"@{TEST_HANDLE} followers={user.get('follower_count')}")
    except Exception as e:
        _record(results, "api_user_info", False, str(e)[:200])
        user = None

    # Video query (last 3 days)
    video_row = None
    try:
        from datetime import datetime as dt, timedelta
        end = dt.now(timezone.utc).strftime("%Y%m%d")
        start = (dt.now(timezone.utc) - timedelta(days=3)).strftime("%Y%m%d")
        chunks = date_chunks(start, end)
        chunk_start, chunk_end = chunks[0]
        videos = query_videos_for_chunk(client, TEST_HANDLE, chunk_start, chunk_end, max_videos=10)
        ok = isinstance(videos, list)
        _record(results, "api_video_query", ok, f"{len(videos)} videos in chunk")
        if videos:
            video_row = videos[0]
    except Exception as e:
        _record(results, "api_video_query", False, str(e)[:200])

    # Comments
    if video_row:
        vid = str(video_row.get("id", ""))
        url = video_row.get("video_url") or f"https://www.tiktok.com/@{TEST_HANDLE}/video/{vid}"
        try:
            comments = get_comments_for_video(client, vid, url, TEST_HANDLE, max_comments=5)
            _record(results, "api_comments", True, f"{len(comments)} comments fetched")
        except Exception as e:
            _record(results, "api_comments", False, str(e)[:200])
    else:
        _record(results, "api_comments", False, "no video to test")

    return video_row


def test_web_hydration(results: List[Dict], video_url: Optional[str]) -> None:
    if not video_url:
        _record(results, "web_hydration", False, "no video URL")
        return
    try:
        web = fetch_web_onscreen_text(video_url)
        err = web.get("error")
        text = (web.get("text") or "").strip()
        ok = not err and len(text) > 0
        _record(results, "web_hydration", ok, f"chars={len(text)}" if ok else (err or "empty"))
    except Exception as e:
        _record(results, "web_hydration", False, str(e)[:200])


def test_text_utils(results: List[Dict]) -> None:
    em = extract_emojis("Hello 🔥 world 🇺🇸")
    merged = merge_visual_text_sources(sticker_overlay_text="Line A", browser_ocr_text="Line B")
    ok = "🔥" in em and "🇺🇸" in em and "Line A" in merged.get("visual_text_combined", "")
    _record(results, "text_emoji_and_merge", ok, f"emojis={em!r}")


def test_optional_deps(results: List[Dict]) -> Dict[str, bool]:
    deps = {}
    for mod, name in [("easyocr", "easyocr"), ("cv2", "opencv"), ("torch", "torch")]:
        try:
            __import__(mod)
            deps[name] = True
        except ImportError:
            deps[name] = False
    import shutil
    deps["ffmpeg"] = shutil.which("ffmpeg") is not None
    for k, v in deps.items():
        _record(results, f"dep_{k}", v, "installed" if v else "missing", extra={"optional": True})
    return deps


def test_download_audio(cfg, results: List[Dict], video_url: Optional[str], video_id: Optional[str]) -> None:
    if not video_url or not video_id:
        _record(results, "download_audio", False, "no video", extra={"optional": True})
        return
    audio_dir = os.path.join(cfg.paths.get("audio_dir", "audio"), "_validation")
    err_info: Dict[str, str] = {}
    path = download_audio(video_url, video_id, audio_dir, error_info=err_info)
    if path and os.path.isfile(path):
        size = os.path.getsize(path)
        try:
            os.remove(path)
        except OSError:
            pass
        _record(results, "download_audio", size > 0, f"{size} bytes", extra={"optional": True})
    else:
        _record(results, "download_audio", False, err_info.get("reason", "failed"), extra={"optional": True})


def run_pipeline_scripts(results: List[Dict], quick: bool, skip_costs: bool) -> None:
    tests = [
        ("script_test_setup", ["scripts/test_setup.py"], 120),
    ]
    if not quick:
        tests += [
            ("script_single_account", [
                "scripts/single_account_run.py", "--handle", TEST_HANDLE,
                "--days", "3", "--max-videos", "5",
            ], 180),
            ("script_pull_videos", [
                "scripts/pull_videos.py", "--group", BATCH_GROUP, "--days", "3",
            ], 180),
            ("script_pull_user_info", [
                "scripts/pull_user_info.py", "--group", BATCH_GROUP,
            ], 180),
            ("script_pull_recent", [
                "scripts/pull_recent_videos.py", "--group", BATCH_GROUP,
                "--max-videos", "3", "--lookback-days", "30",
            ], 180),
            ("script_enrich_web", [
                "scripts/enrich_videos_with_ocr.py", "--group", BATCH_GROUP,
                "--web-only", "--limit", "5", "--force",
            ], 180),
            ("script_enrich_merge", [
                "scripts/enrich_videos_with_ocr.py", "--group", BATCH_GROUP,
                "--merge-only", "--limit", "20",
            ], 120),
            ("script_export_videos", [
                "scripts/export_csv.py", "--group", BATCH_GROUP, "--videos",
            ], 60),
            ("script_export_users", [
                "scripts/export_csv.py", "--group", BATCH_GROUP, "--users",
            ], 60),
            ("script_debug_payload", [
                "scripts/debug_api_video_payload.py",
                "--username", TEST_HANDLE, "--compare-web",
            ], 120),
        ]
        if not skip_costs:
            tests += [
                ("script_classify_videos", [
                    "scripts/classify_videos.py", "--group", BATCH_GROUP, "--days", "3",
                ], 300),
                ("script_classify_accounts", [
                    "scripts/classify_accounts.py", "--group", BATCH_GROUP,
                ], 180),
                ("script_download_transcribe", [
                    "scripts/download_and_transcribe.py", "--group", BATCH_GROUP,
                    "--start-date",
                    (datetime.now(timezone.utc) - __import__("datetime").timedelta(days=7)).strftime("%Y-%m-%d"),
                    "--max-recent", "1", "--workers", "1",
                ], 300),
            ]

    for name, args, timeout in tests:
        ok, tail = _run_script(args, timeout=timeout)
        _record(results, name, ok, tail.replace("\n", " | ")[:300])


def main() -> int:
    parser = argparse.ArgumentParser(description="Full pipeline validation")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--quick", action="store_true", help="Infra + API only")
    parser.add_argument("--skip-openai-costs", action="store_true", help="Skip classify/transcribe")
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)

    print("=" * 60)
    print("FULL VALIDATION — TikTok Research Pipeline")
    print("=" * 60)

    results: List[Dict[str, Any]] = []
    t0 = time.perf_counter()

    print("\n[1/4] Infrastructure")
    test_resolv_conf(results)
    test_dns(results)
    test_tls(results)
    test_www_tiktok(results)

    print("\n[2/4] API layer")
    cfg = load_config(args.config)
    video = test_api_layer(cfg, results)
    video_url = None
    video_id = None
    if video:
        video_id = str(video.get("id", ""))
        video_url = video.get("video_url") or (
            f"https://www.tiktok.com/@{TEST_HANDLE}/video/{video_id}" if video_id else None
        )

    print("\n[3/4] Web hydration + text utils")
    test_web_hydration(results, video_url)
    test_text_utils(results)

    print("\n[4/4] Pipeline scripts + optional deps")
    deps = test_optional_deps(results)
    if video_url and video_id:
        test_download_audio(cfg, results, video_url, video_id)
    run_pipeline_scripts(results, quick=args.quick, skip_costs=args.skip_openai_costs)

    elapsed = time.perf_counter() - t0
    critical = [r for r in results if not r.get("optional")]
    optional = [r for r in results if r.get("optional")]
    crit_pass = sum(1 for r in critical if r["ok"])
    opt_pass = sum(1 for r in optional if r["ok"])

    report = {
        "generated_at": _now(),
        "elapsed_seconds": round(elapsed, 1),
        "critical_pass": f"{crit_pass}/{len(critical)}",
        "optional_pass": f"{opt_pass}/{len(optional)}",
        "all_critical_pass": crit_pass == len(critical),
        "results": results,
        "deps": deps,
    }
    os.makedirs("data", exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 60)
    print(f"CRITICAL: {crit_pass}/{len(critical)} passed")
    print(f"OPTIONAL: {opt_pass}/{len(optional)} passed")
    print(f"Elapsed: {elapsed:.1f}s")
    print(f"Report: {REPORT_PATH}")
    print("=" * 60)

    failed = [r["name"] for r in critical if not r["ok"]]
    if failed:
        print("FAILED:", ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
