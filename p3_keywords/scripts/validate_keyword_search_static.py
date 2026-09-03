"""Static/local Pipeline 3 checks. No TikTok API, media, or BigQuery writes."""

from __future__ import annotations

import csv
import os
import socket
import sys
import tempfile
from pathlib import Path
from typing import Any, List
from unittest.mock import patch

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

SAMPLE_EXPECTED = ["news", "trump", "tsa", "ice", "netanyahu"]
FIRST_FIVE_OF_FILE = ["trump", "iran", "war", "says", "state"]
CANONICAL_CSV = Path.home() / "Downloads" / "march_news_keywords.csv"


def _fail(name: str, msg: str) -> None:
    raise AssertionError(f"{name}: {msg}")


def _load_csv_terms(path: Path) -> List[str]:
    terms: List[str] = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            t = (row.get("term") or "").strip()
            if t:
                terms.append(t)
    return terms


def main() -> int:
    os.environ.pop("TIKTOK_ALLOW_LOCAL_COLLECTION", None)

    api_calls: List[Any] = []

    def _blocked_post(*args, **kwargs):
        api_calls.append((args, kwargs))
        raise AssertionError("production TikTok API call during static tests")

    with patch("requests.post", side_effect=_blocked_post):
        _run_checks()

    if api_calls:
        _fail("10. no production API call", f"requests.post invoked {len(api_calls)} time(s)")
    print("PASS 10. no production API call during tests")
    print("All Pipeline 3 static checks passed.")
    return 0


def _run_checks() -> None:
    os.environ.setdefault("TIKTOK_CLIENT_KEY", "static-test-placeholder")
    os.environ.setdefault("TIKTOK_CLIENT_SECRET", "static-test-placeholder")
    os.environ.setdefault("OPENAI_API_KEY", "static-test-placeholder")
    from tiktok.checkpoint import CheckpointStore
    from tiktok.collection.daily_keyword_pipeline import (
        apply_chunk_checkpoint,
        query_keyword_chunk_with_retries,
    )
    from tiktok.collection.server_guard import require_collection_server
    from tiktok.config import load_config
    from tiktok.db import get_connection, merge_matched_keywords, parse_matched_keywords, upsert_collected_video
    from enrichment.bigquery_loader import (
        BQ_SCHEMAS,
        CONTENT_CREATORS_TABLE,
        KEYWORD_SEARCH_TABLE,
        NEWS_ACCOUNTS_TABLE,
        keyword_search_schema_spec,
    )
    from tiktok.pipelines import (
        PIPELINE_CONTENT_CREATORS,
        PIPELINE_KEYWORD_SEARCH,
        PIPELINE_NEWS_ACCOUNTS,
        get_pipeline,
        load_handle_file,
    )

    cfg = load_config("common/config.yaml")
    p3 = get_pipeline(cfg, PIPELINE_KEYWORD_SEARCH)
    p1 = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
    p2 = get_pipeline(cfg, PIPELINE_NEWS_ACCOUNTS)

    # 1. 263 keywords and order
    terms = p3.resolve_keywords(cfg, sample=False)
    file_terms_path = ROOT / "p3_keywords/config/march_news_keywords.txt"
    if not file_terms_path.is_file():
        _fail("1. 263 keywords", f"missing {file_terms_path}")
    if len(terms) != 263:
        _fail("1. 263 keywords", f"got {len(terms)} terms")
    if len(set(terms)) != 263:
        _fail("1. 263 keywords", "duplicate terms in canonical list")
    if CANONICAL_CSV.is_file():
        csv_terms = _load_csv_terms(CANONICAL_CSV)
        if terms != csv_terms:
            _fail("1. 263 keywords", "repo order does not match march_news_keywords.csv")
    dup_path = ROOT / "archive/discovery/config/mediacloud_march_2026.txt"
    dup_terms = [
        line.strip()
        for line in dup_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    if terms != dup_terms:
        _fail("1. 263 keywords", "mediacloud_march_2026.txt is not an identical term list")
    print("PASS 1. 263 keywords and original order")

    # 2. Sample keywords
    sample = p3.resolve_keywords(cfg, sample=True)
    if sample != SAMPLE_EXPECTED:
        _fail("2. sample keywords", f"got {sample}")
    if sample == FIRST_FIVE_OF_FILE:
        _fail("2. sample keywords", "sample is the first five file terms")
    missing = [t for t in sample if t not in terms]
    if missing:
        _fail("2. sample keywords", f"not in canonical list: {missing}")
    print("PASS 2. sample keywords news, trump, tsa, ice, netanyahu")

    # 3. P1/P2 exclusion from handle files, not YAML groups
    if p3.exclude_handle_groups:
        _fail("3. exclusion files", f"YAML groups still configured: {p3.exclude_handle_groups}")
    sets = p3.exclusion_handle_sets(cfg)
    p1_file = load_handle_file(str(ROOT / "p1_content_creators/config/newsfluencer_combined.txt"))
    p2_file = load_handle_file(str(ROOT / "p2_news/config/news_accounts.txt"))
    if sets.get("pipeline_1") != p1_file:
        _fail("3. exclusion files", "pipeline_1 set is not newsfluencer_combined.txt")
    if sets.get("pipeline_2") != p2_file:
        _fail("3. exclusion files", "pipeline_2 set is not news_accounts.txt")
    if len(p1_file) != 526:
        _fail("3. exclusion files", f"P1 file has {len(p1_file)} handles, expected 526")
    if len(p2_file) != 137:
        _fail("3. exclusion files", f"P2 file has {len(p2_file)} handles, expected 137")
    yaml_complete = [h for h in (cfg.handle_groups.get("complete") or []) if h]
    if len(p1_file) == len({h.strip().lstrip('@').lower() for h in yaml_complete}):
        _fail("3. exclusion files", "P1 exclusion count matches YAML complete; expected handle file")
    excluded = p3.exclusion_handles(cfg)
    if excluded != set(p1_file) | set(p2_file):
        _fail("3. exclusion files", "combined exclusion set mismatch")
    print("PASS 3. P1/P2 exclusion loaded from handle files")

    # 4. matched_keywords merging
    if merge_matched_keywords(["news"], "trump") != ["news", "trump"]:
        _fail("4. matched_keywords", "list+str merge failed")
    if merge_matched_keywords('["news","trump"]', ["trump", "ice"]) != ["news", "trump", "ice"]:
        _fail("4. matched_keywords", "json merge failed")
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "t.db")
        conn = get_connection(db_path)
        base = {
            "video_id": "123",
            "username": "someone",
            "video_url": "https://www.tiktok.com/@someone/video/123",
            "create_time": 1,
            "posted_at": "",
            "caption": "",
            "hashtags": "",
            "like_count": 0,
            "share_count": 0,
            "comment_count": 0,
            "save_count": 0,
            "duration_seconds": 0,
            "pipeline_id": "keyword",
            "collection_source": "keyword",
            "api_source": "KEYWORD_SEARCH_API",
        }
        upsert_collected_video(conn, {**base, "matched_keyword": "news"})
        upsert_collected_video(conn, {**base, "matched_keyword": "trump"})
        raw = conn.execute(
            "SELECT matched_keywords FROM videos WHERE video_id='123'"
        ).fetchone()[0]
        conn.close()
        if parse_matched_keywords(raw) != ["news", "trump"]:
            _fail("4. matched_keywords", f"sqlite merge got {raw!r}")
    print("PASS 4. matched_keywords merging")

    # 5. No credential fallback
    saved = {
        k: os.environ.get(k)
        for k in (
            "KEYWORD_SEARCH_API_CLIENT_KEY",
            "KEYWORD_SEARCH_API_CLIENT_SECRET",
            "TIKTOK_CLIENT_KEY",
            "TIKTOK_CLIENT_SECRET",
            "NEWS_API_CLIENT_KEY",
            "NEWS_API_CLIENT_SECRET",
            "CONTENT_CREATOR_TIKTOK_CLIENT_KEY",
            "CONTENT_CREATOR_TIKTOK_CLIENT_SECRET",
        )
    }
    try:
        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_KEY", None)
        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_SECRET", None)
        os.environ["TIKTOK_CLIENT_KEY"] = "p1-key-should-not-be-used"
        os.environ["TIKTOK_CLIENT_SECRET"] = "p1-secret-should-not-be-used"
        os.environ["NEWS_API_CLIENT_KEY"] = "p2-key-should-not-be-used"
        os.environ["NEWS_API_CLIENT_SECRET"] = "p2-secret-should-not-be-used"
        try:
            p3.resolve_credentials(cfg)
            _fail("5. no credential fallback", "resolve_credentials succeeded without P3 env vars")
        except RuntimeError as e:
            msg = str(e)
            if "KEYWORD_SEARCH_API_CLIENT_KEY" not in msg:
                _fail("5. no credential fallback", f"unexpected error: {msg}")
            if "will not fall back to Pipeline 1 or Pipeline 2" not in msg:
                _fail("5. no credential fallback", f"error did not refuse P1/P2 fallback: {msg}")
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    print("PASS 5. P3 cannot fall back to other credentials")

    # 6. Mac host guard
    host = socket.gethostname().lower()
    if "cme-p01" in host:
        print("SKIP 6. Mac host guard (already on collection server)")
    else:
        try:
            require_collection_server()
            _fail("6. Mac host guard", "require_collection_server() did not exit")
        except SystemExit as e:
            if "Refusing TikTok collection" not in str(e):
                _fail("6. Mac host guard", f"unexpected exit: {e}")
        print("PASS 6. Mac host guard")

    # 7. P1/P2 default destinations unchanged
    if p1.bigquery_table != "content_creators":
        _fail("7. P1/P2 destinations", f"P1 table is {p1.bigquery_table}")
    if p2.bigquery_table != "news":
        _fail("7. P1/P2 destinations", f"P2 table is {p2.bigquery_table}")
    if p3.bigquery_table != "keyword":
        _fail("7. P1/P2 destinations", f"P3 table is {p3.bigquery_table}")
    if p1.effective_max_videos() is not None:
        pass  # P1 unused
    if p3.effective_max_videos() is not None:
        _fail("7. P1/P2 destinations", f"P3 still has a video cap: {p3.effective_max_videos()}")
    print("PASS 7. P1/P2 default destinations unchanged; P3 has no hidden video cap")

    # 8. P3 schema in code, no production table
    spec = keyword_search_schema_spec()
    mk = spec.get("matched_keywords") or {}
    if mk.get("type") != "STRING" or mk.get("mode") != "REPEATED":
        _fail("8. P3 schema", f"matched_keywords spec is {mk}")
    p1_names = [f["name"] for f in BQ_SCHEMAS[CONTENT_CREATORS_TABLE]]
    p2_names = [f["name"] for f in BQ_SCHEMAS[NEWS_ACCOUNTS_TABLE]]
    p3_names = spec["fields"]
    if "matched_keywords" in p1_names or "matched_keywords" in p2_names:
        _fail("8. P3 schema", "matched_keywords leaked into P1/P2 schema")
    if p3_names[-1] != "matched_keywords":
        _fail("8. P3 schema", "matched_keywords is not on keyword")
    for name in ("voice_to_text", "whisper_transcript", "ocr_text", "emoji_characters", "pipeline_id"):
        if name not in p3_names:
            _fail("8. P3 schema", f"missing {name}")
    if KEYWORD_SEARCH_TABLE != "keyword":
        _fail("8. P3 schema", "table constant mismatch")
    print("PASS 8. P3 BigQuery schema in code (table not created)")

    # 9. HTTP 500 retry / checkpoint
    calls = {"n": 0}

    def _always_500(*args, **kwargs):
        calls["n"] += 1
        raise RuntimeError("video/query HTTP error for keyword='news' 20260813-20260814")

    result = query_keyword_chunk_with_retries(
        client=None,
        keyword="news",
        chunk_start="20260813",
        chunk_end="20260814",
        retry_attempts=3,
        sleep_seconds=0,
        query_fn=_always_500,
    )
    if not result.get("failed") or result.get("abort") or result.get("attempts") != 3:
        _fail("9. HTTP 500 retry", f"result={result}")
    if calls["n"] != 3:
        _fail("9. HTTP 500 retry", f"called {calls['n']} times, expected 3")
    with tempfile.TemporaryDirectory() as tmp:
        ckpt = CheckpointStore(os.path.join(tmp, "keyword_search_2026-08-13.json"))
        apply_chunk_checkpoint(ckpt, "news", "20260813", "20260814", result)
        if ckpt.is_done("news", "20260813", "20260814"):
            _fail("9. HTTP 500 retry", "checkpointed as completed")
        if not ckpt.is_failed("news", "20260813", "20260814"):
            _fail("9. HTTP 500 retry", "not checkpointed as failed")
        abort_result = {
            "videos": None,
            "abort": "rate_limited",
            "failed": False,
            "attempts": 1,
            "error": RuntimeError("rate_limited HTTP 429"),
        }
        apply_chunk_checkpoint(ckpt, "trump", "20260813", "20260814", abort_result)
        if ckpt.is_done("trump", "20260813", "20260814") or ckpt.is_failed(
            "trump", "20260813", "20260814"
        ):
            _fail("9. HTTP 500 retry", "429/quota must preserve unset checkpoint")
    print("PASS 9. HTTP 500 retry 3x → failed; 429 preserves checkpoints")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as e:
        print(f"FAIL {e}", file=sys.stderr)
        raise SystemExit(1)
