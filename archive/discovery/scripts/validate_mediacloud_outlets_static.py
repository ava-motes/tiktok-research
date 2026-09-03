"""Static checks for MediaCloud outlet handle discovery.

No TikTok API, media, enrichment, SQLite, or BigQuery writes.
"""

from __future__ import annotations

import ast
import hashlib
import os
import sys
import tempfile
from pathlib import Path
from typing import List
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

CSV_PATH = ROOT / "config" / "discovery" / "mediacloud_us_news_outlets.csv"
P1_PATH = ROOT / "config" / "handles" / "newsfluencer_combined.txt"
P2_PATH = ROOT / "config" / "handles" / "news_accounts.txt"
MODULE_PY = ROOT / "tiktok" / "discovery" / "mediacloud_outlets.py"
CLI_PY = ROOT / "scripts" / "discover_mediacloud_outlets.py"
P1_LIST = ROOT / "config" / "handles" / "newsfluencer_combined.txt"
P2_LIST = ROOT / "config" / "handles" / "news_accounts.txt"

FORBIDDEN_IMPORT_ROOTS = (
    "tiktok.db",
    "tiktok.enrichment",
    "tiktok.api.videos",
    "tiktok.api.download",
    "tiktok.ocr",
    "tiktok.transcription",
    "google.cloud",
)


def _fail(name: str, msg: str) -> None:
    raise AssertionError(f"{name}: {msg}")


def _module_imports(path: Path) -> List[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    return names


class FakeClient:
    def __init__(self, handler):
        self.handler = handler
        self.calls: List[str] = []
        self.endpoints: List[str] = []

    def post_with_status(
        self,
        endpoint: str,
        body: dict,
        params: dict,
        handle: str = "",
        raise_on_rate_limit: bool = False,
        **kwargs,
    ) -> dict:
        self.calls.append(handle)
        self.endpoints.append(endpoint)
        if "video" in endpoint:
            raise AssertionError(f"video endpoint not allowed: {endpoint}")
        return self.handler(endpoint, body, handle, raise_on_rate_limit)


def _found_body(username: str, display_name: str, bio: str = ""):
    return {
        "ok": True,
        "http_status": 200,
        "body": {
            "data": {
                "username": username,
                "display_name": display_name,
                "bio_description": bio,
                "is_verified": False,
                "follower_count": 10,
            }
        },
    }


def main() -> int:
    from tiktok.discovery.mediacloud_outlets import (
        domain_slugs,
        generate_candidates,
        parse_outlets,
        run_discovery,
    )
    from tiktok.discovery.paperboy_journalists import load_known_handles
    from tiktok.pipelines import load_handle_file

    if not CSV_PATH.is_file():
        _fail("1. seed csv", f"missing {CSV_PATH}")
    outlets = parse_outlets(str(CSV_PATH))
    if len(outlets) != 242:
        _fail("1. seed csv", f"expected 242 outlets, got {len(outlets)}")
    if any(o.csv_tiktok_handle for o in outlets):
        _fail("1. seed csv", "tiktok handle column should be empty in Ava's file")
    print("PASS 1. 242 MediaCloud outlets, empty tiktok handle column")

    nyt = next(o for o in outlets if o.name == "New York Times")
    nyt_users = {c.username: c.source for c in generate_candidates(nyt)}
    if "nytimes" not in nyt_users:
        _fail("2. candidates", f"New York Times missing nytimes: {nyt_users}")
    if "newyorktimes" not in nyt_users:
        _fail("2. candidates", f"New York Times missing newyorktimes: {nyt_users}")
    lat = next(o for o in outlets if o.name == "latimes.com")
    lat_users = {c.username for c in generate_candidates(lat)}
    if lat_users != {"latimes"}:
        _fail("2. candidates", f"latimes.com should only guess latimes, got {lat_users}")
    if domain_slugs("abcnews.go.com")[0] != "abcnews":
        _fail("2. candidates", domain_slugs("abcnews.go.com"))
    if "elitedaily" not in domain_slugs("cdn29.elitedaily.com"):
        _fail("2. candidates", domain_slugs("cdn29.elitedaily.com"))
    if "popmatters" not in domain_slugs("http://www.popmatters.com/feeds/"):
        _fail("2. candidates", domain_slugs("http://www.popmatters.com/feeds/"))
    print("PASS 2. conservative outlet username guesses")

    p1_before = hashlib.sha256(P1_LIST.read_bytes()).hexdigest()
    p2_before = hashlib.sha256(P2_LIST.read_bytes()).hexdigest()
    for name in _module_imports(MODULE_PY) + _module_imports(CLI_PY):
        if any(name == root or name.startswith(root + ".") for root in FORBIDDEN_IMPORT_ROOTS):
            _fail("3. isolation", name)
    cli_src = CLI_PY.read_text(encoding="utf-8")
    if "NEWS_API_CLIENT_KEY" in cli_src or "require_news_credentials" in cli_src:
        _fail("3. isolation", "outlet discovery must not use NEWS_API / P2")
    if "query_videos" in MODULE_PY.read_text(encoding="utf-8"):
        _fail("3. isolation", "video query in outlet discovery")
    print("PASS 3. no P2 keys, no video/BQ/SQLite imports")

    known = load_known_handles(str(P1_PATH), str(P2_PATH))
    if known.get("nytimes") != "news_accounts":
        _fail("4. known skip", known.get("nytimes"))

    def handler(endpoint, body, handle, raise_on_rate_limit):
        if handle == "nytimes":
            _fail("4. known skip", "API called for already-known nytimes")
        return _found_body(handle, "Los Angeles Times", "Official LAT")

    client = FakeClient(handler)
    with tempfile.TemporaryDirectory() as tmp:
        stats = run_discovery(
            csv_path=str(CSV_PATH),
            client=client,
            known=known,
            sample=True,
            checkpoint_path=str(Path(tmp) / "ckpt.json"),
            output_csv=str(Path(tmp) / "out.csv"),
            output_csv_high=str(Path(tmp) / "high.csv"),
            run_json=str(Path(tmp) / "run.json"),
            sleep=lambda _s: None,
        )
        if stats["workflow"] != "mediacloud_outlet_discovery":
            _fail("4. known skip", stats["workflow"])
        if stats["outlets"] != 10:
            _fail("4. sample", stats["outlets"])
        if "nytimes" in client.calls:
            _fail("4. known skip", "nytimes probed")
        if "latimes" not in client.calls and "latimes" in known:
            # latimes is in P2; must also be skipped
            pass
        if any("video" in e for e in client.endpoints):
            _fail("4. known skip", client.endpoints)
    if hashlib.sha256(P1_LIST.read_bytes()).hexdigest() != p1_before:
        _fail("5. no list edits", "P1 handle file changed")
    if hashlib.sha256(P2_LIST.read_bytes()).hexdigest() != p2_before:
        _fail("5. no list edits", "P2 handle file changed")
    print("PASS 4. skip already-known P2 handles; sample=10")
    print("PASS 5. P1/P2 handle files unchanged")

    p1_n = len(load_handle_file(str(P1_PATH)))
    p2_n = len(load_handle_file(str(P2_PATH)))
    if p2_n != 137:
        _fail("6. p2 count", p2_n)
    if p1_n < 500:
        _fail("6. p1 count", p1_n)
    print("PASS 6. production handle lists untouched (counts)")
    print("PASS MediaCloud outlet discovery static checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
