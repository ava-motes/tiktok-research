"""Static/local checks for three isolated production pipelines.

No TikTok API, media, Whisper/OCR/emoji, or BigQuery writes.
"""

from __future__ import annotations

import ast
import csv
import json
import os
import re
import socket
import subprocess
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

SAMPLE_KEYWORDS = ["news", "trump", "tsa", "ice", "netanyahu"]
FIRST_FIVE_OF_FILE = ["trump", "iran", "war", "says", "state"]
CANONICAL_CSV = Path.home() / "Downloads" / "march_news_keywords.csv"
P1_P2_OVERLAP = {"btnewsroom", "therecount", "treyyingst"}

P1_FORBIDDEN = frozenset(
    {
        "NEWS_API_CLIENT_KEY",
        "NEWS_API_CLIENT_SECRET",
        "KEYWORD_SEARCH_API_CLIENT_KEY",
        "KEYWORD_SEARCH_API_CLIENT_SECRET",
    }
)


def _fail(name: str, msg: str) -> None:
    raise AssertionError(f"{name}: {msg}")


def _load_csv_terms(path: Path) -> List[str]:
    terms: List[str] = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            t = (row.get("term") or "").strip().lower()
            if t:
                terms.append(t)
    out: List[str] = []
    seen = set()
    for t in terms:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _restore(saved) -> None:
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def main() -> int:
    os.environ.pop("TIKTOK_ALLOW_LOCAL_COLLECTION", None)
    api_calls: List[Any] = []

    def _blocked_post(*args, **kwargs):
        api_calls.append((args, kwargs))
        raise AssertionError("TikTok/API HTTP during static tests")

    with patch("requests.post", side_effect=_blocked_post):
        _run_checks()

    if api_calls:
        _fail("20. no API calls", f"requests.post invoked {len(api_calls)} time(s)")
    print("PASS 20. no API calls during tests")
    print("All three-pipeline static checks passed.")
    return 0


def _run_checks() -> None:
    from tiktok.checkpoint import CheckpointStore
    from tiktok.collection.daily_keyword_pipeline import (
        _classify_exclusion,
        abort_reason,
        apply_chunk_checkpoint,
        query_keyword_chunk_with_retries,
    )
    from tiktok.collection.daily_handle_pipeline import _abort_reason
    from tiktok.collection.date_window import research_window
    from tiktok.collection.server_guard import require_collection_server
    from tiktok.config import load_config
    from tiktok.db import (
        get_connection,
        merge_matched_keywords,
        parse_matched_keywords,
        upsert_collected_video,
    )
    from enrichment.bigquery_loader import (
        BQ_SCHEMAS,
        CONTENT_CREATORS_TABLE,
        ENRICHED_TABLE,
        KEYWORD_TABLE,
        NEWS_TABLE,
        keyword_search_schema_spec,
    )
    from enrichment.validate_row import (
        validate_keyword_search_row,
        validate_news_account_row,
        validate_pipeline_row,
    )
    from tiktok.pipelines import (
        KEYWORD_SEARCH_CLIENT_KEY_ENV,
        KEYWORD_SEARCH_CLIENT_SECRET_ENV,
        NEWS_CLIENT_KEY_ENV,
        NEWS_CLIENT_SECRET_ENV,
        P2_FORBIDDEN_CREDENTIAL_ENVS,
        P3_FORBIDDEN_CREDENTIAL_ENVS,
        PIPELINE_CONTENT_CREATORS,
        PIPELINE_KEYWORD,
        PIPELINE_NEWS,
        get_pipeline,
        load_handle_file,
        load_keyword_file,
        require_keyword_search_credentials,
        require_news_credentials,
    )

    cfg = load_config("common/config.yaml")
    p1 = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
    p2 = get_pipeline(cfg, PIPELINE_NEWS)
    p3 = get_pipeline(cfg, PIPELINE_KEYWORD)

    p1_handles = load_handle_file(str(ROOT / "p1_content_creators/config/newsfluencer_combined.txt"))
    p2_handles = load_handle_file(str(ROOT / "p2_news/config/news_accounts.txt"))
    keywords = load_keyword_file(str(ROOT / "p3_keywords/config/march_news_keywords.txt"))

    if p1.handle_file != "p1_content_creators/config/newsfluencer_combined.txt":
        _fail("1. input lists", f"P1 handle_file={p1.handle_file}")
    if p2.handle_file != "p2_news/config/news_accounts.txt":
        _fail("1. input lists", f"P2 handle_file={p2.handle_file}")
    if p3.keyword_file != "p3_keywords/config/march_news_keywords.txt":
        _fail("1. input lists", f"P3 keyword_file={p3.keyword_file}")
    if p1.resolve_handles(cfg, sample=False) != p1_handles:
        _fail("1. input lists", "P1 resolved handles != newsfluencer_combined.txt")
    if p2.resolve_handles(cfg, sample=False) != p2_handles:
        _fail("1. input lists", "P2 resolved handles != news_accounts.txt")
    if p3.resolve_keywords(cfg, sample=False) != keywords:
        _fail("1. input lists", "P3 resolved keywords != march_news_keywords.txt")
    print("PASS 1. correct input lists")

    if len(p1_handles) != 526 or len(set(p1_handles)) != 526:
        _fail("2. counts", f"P1 unique handles={len(p1_handles)}")
    if len(p2_handles) != 137 or len(set(p2_handles)) != 137:
        _fail("2. counts", f"P2 unique handles={len(p2_handles)}")
    if len(keywords) != 263 or len(set(keywords)) != 263:
        _fail("2. counts", f"P3 unique keywords={len(keywords)}")
    if CANONICAL_CSV.is_file() and keywords != _load_csv_terms(CANONICAL_CSV):
        _fail("2. counts", "keyword file does not match march_news_keywords.csv")
    sample_kw = p3.resolve_keywords(cfg, sample=True)
    if sample_kw != SAMPLE_KEYWORDS:
        _fail("2. counts", f"sample keywords={sample_kw}")
    if sample_kw == FIRST_FIVE_OF_FILE:
        _fail("2. counts", "sample is first five file terms")
    sample_p2 = p2.resolve_handles(cfg, sample=True)
    if sample_p2 != ["6abcactionnews", "nbcnewyork"]:
        _fail("2. counts", f"P2 sample={sample_p2}")
    print("PASS 2. 526 / 137 / 263 normalized counts")

    if p1.client_key_env != "CONTENT_CREATOR_TIKTOK_CLIENT_KEY":
        _fail("3. credential env", f"P1 key env={p1.client_key_env}")
    if p2.client_key_env != NEWS_CLIENT_KEY_ENV:
        _fail("3. credential env", f"P2 key env={p2.client_key_env}")
    if p2.client_secret_env != NEWS_CLIENT_SECRET_ENV:
        _fail("3. credential env", f"P2 secret env={p2.client_secret_env}")
    if p3.client_key_env != KEYWORD_SEARCH_CLIENT_KEY_ENV:
        _fail("3. credential env", f"P3 key env={p3.client_key_env}")
    if p3.client_secret_env != KEYWORD_SEARCH_CLIENT_SECRET_ENV:
        _fail("3. credential env", f"P3 secret env={p3.client_secret_env}")
    if not p2.require_dedicated_credentials or not p3.require_dedicated_credentials:
        _fail("3. credential env", "P2/P3 must require dedicated credentials")
    print("PASS 3. credential environment variables")

    saved = {
        k: os.environ.get(k)
        for k in (
            "TIKTOK_CLIENT_KEY",
            "TIKTOK_CLIENT_SECRET",
            "CONTENT_CREATOR_TIKTOK_CLIENT_KEY",
            "CONTENT_CREATOR_TIKTOK_CLIENT_SECRET",
            "NEWS_API_CLIENT_KEY",
            "NEWS_API_CLIENT_SECRET",
            "KEYWORD_SEARCH_API_CLIENT_KEY",
            "KEYWORD_SEARCH_API_CLIENT_SECRET",
        )
    }
    try:
        os.environ["TIKTOK_CLIENT_KEY"] = "p1-key"
        os.environ["TIKTOK_CLIENT_SECRET"] = "p1-secret"
        os.environ["CONTENT_CREATOR_TIKTOK_CLIENT_KEY"] = ""
        os.environ["CONTENT_CREATOR_TIKTOK_CLIENT_SECRET"] = ""
        os.environ["NEWS_API_CLIENT_KEY"] = "p2-key"
        os.environ["NEWS_API_CLIENT_SECRET"] = "p2-secret"
        os.environ["KEYWORD_SEARCH_API_CLIENT_KEY"] = "p3-key"
        os.environ["KEYWORD_SEARCH_API_CLIENT_SECRET"] = "p3-secret"
        cfg = load_config("common/config.yaml")
        p1 = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
        p2 = get_pipeline(cfg, PIPELINE_NEWS)
        p3 = get_pipeline(cfg, PIPELINE_KEYWORD)

        if p1.resolve_credentials(cfg) != ("p1-key", "p1-secret"):
            _fail("4. no fallback", "P1 did not use TIKTOK_CLIENT_*")
        if p2.resolve_credentials(cfg) != ("p2-key", "p2-secret"):
            _fail("4. no fallback", "P2 did not use NEWS_API_*")
        if p3.resolve_credentials(cfg) != ("p3-key", "p3-secret"):
            _fail("4. no fallback", "P3 did not use KEYWORD_SEARCH_API_*")

        real_get = os.environ.get

        def _spy(forbidden):
            reads: List[str] = []

            def _get(key, default=None):
                if key in forbidden:
                    reads.append(str(key))
                return real_get(key, default)

            return reads, _get

        p1_reads, p1_get = _spy(P1_FORBIDDEN)
        with patch.object(os.environ, "get", side_effect=p1_get):
            p1.resolve_credentials(cfg)
        if p1_reads:
            _fail("4. no fallback", f"P1 read P2/P3 env: {p1_reads}")

        p2_reads, p2_get = _spy(P2_FORBIDDEN_CREDENTIAL_ENVS)
        with patch.object(os.environ, "get", side_effect=p2_get):
            p2.resolve_credentials(cfg)
            require_news_credentials(p2)
        if p2_reads:
            _fail("4. no fallback", f"P2 read P1/P3 env: {p2_reads}")

        p3_reads, p3_get = _spy(P3_FORBIDDEN_CREDENTIAL_ENVS)
        with patch.object(os.environ, "get", side_effect=p3_get):
            p3.resolve_credentials(cfg)
            require_keyword_search_credentials(p3)
        if p3_reads:
            _fail("4. no fallback", f"P3 read P1/P2 env: {p3_reads}")

        os.environ.pop("NEWS_API_CLIENT_KEY", None)
        os.environ.pop("NEWS_API_CLIENT_SECRET", None)
        try:
            p2.resolve_credentials(cfg)
            _fail("4. no fallback", "P2 succeeded without NEWS_API_*")
        except RuntimeError as e:
            if "NEWS_API_CLIENT_KEY" not in str(e):
                _fail("4. no fallback", f"P2 missing-cred error: {e}")
            if "will not fall back" not in str(e):
                _fail("4. no fallback", f"P2 error did not refuse fallback: {e}")

        os.environ["NEWS_API_CLIENT_KEY"] = "p2-key"
        os.environ["NEWS_API_CLIENT_SECRET"] = "p2-secret"
        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_KEY", None)
        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_SECRET", None)
        try:
            p3.resolve_credentials(cfg)
            _fail("4. no fallback", "P3 succeeded without KEYWORD_SEARCH_API_*")
        except RuntimeError as e:
            if "KEYWORD_SEARCH_API_CLIENT_KEY" not in str(e):
                _fail("4. no fallback", f"P3 missing-cred error: {e}")

        os.environ["TIKTOK_CLIENT_KEY"] = ""
        os.environ["TIKTOK_CLIENT_SECRET"] = ""
        os.environ["CONTENT_CREATOR_TIKTOK_CLIENT_KEY"] = ""
        os.environ["CONTENT_CREATOR_TIKTOK_CLIENT_SECRET"] = ""
        cfg_empty_p1 = load_config("common/config.yaml")
        p1_empty = get_pipeline(cfg_empty_p1, PIPELINE_CONTENT_CREATORS)
        try:
            p1_empty.resolve_credentials(cfg_empty_p1)
            _fail("4. no fallback", "P1 succeeded without TIKTOK_CLIENT_*")
        except RuntimeError:
            pass
    finally:
        _restore(saved)
    print("PASS 4. no credential fallback across P1/P2/P3")

    host = socket.gethostname().lower()
    if "cme-p01" in host:
        print("SKIP 5. host guard (already on collection server)")
    else:
        try:
            require_collection_server()
            _fail("5. host guard", "require_collection_server() did not exit")
        except SystemExit as e:
            if "Refusing TikTok collection" not in str(e):
                _fail("5. host guard", f"unexpected exit: {e}")
        with patch(
            "tiktok.collection.server_guard.socket.gethostname",
            return_value="comm-a92978",
        ):
            try:
                require_collection_server()
                _fail("5. host guard", "comm-a92978 was allowed")
            except SystemExit as e:
                if "comm-a92978" not in str(e).lower():
                    _fail("5. host guard", f"exit did not name comm-a92978: {e}")
        for rel in (
            "p1_content_creators/scripts/run_content_creators.py",
            "p2_news/scripts/run_news.py",
            "p3_keywords/scripts/run_keyword.py",
            "p1_content_creators/scripts/collect_content_creators.py",
            "p2_news/scripts/collect_news.py",
            "p3_keywords/scripts/collect_keyword.py",
        ):
            body = (ROOT / rel).read_text(encoding="utf-8")
            if "require_collection_server()" not in body:
                _fail("5. host guard", f"{rel} missing require_collection_server()")
        print("PASS 5. host guard (Mac + comm-a92978)")

    window = research_window("2026-08-13", timezone_name="America/Chicago")
    if window.research_date != "2026-08-13":
        _fail("6. date window", f"research_date={window.research_date}")
    if "2026-08-13" not in window.collection_window_start:
        _fail("6. date window", f"start={window.collection_window_start}")
    if "2026-08-14" not in window.collection_window_end:
        _fail("6. date window", f"end={window.collection_window_end}")
    if window.api_start_yyyymmdd != "20260813":
        _fail("6. date window", f"api_start={window.api_start_yyyymmdd}")
    if window.api_end_yyyymmdd != "20260814":
        _fail("6. date window", f"api_end={window.api_end_yyyymmdd}")
    if window.api_query_chunks() != [("20260813", "20260814")]:
        _fail("6. date window", f"chunks={window.api_query_chunks()}")
    print("PASS 6. Chicago date window")

    from tiktok.collection.date_window import utc_calendar_window

    utc_win = utc_calendar_window("2026-08-13")
    if utc_win.api_start_yyyymmdd != "20260813" or utc_win.api_end_yyyymmdd != "20260813":
        _fail(
            "6b. utc day",
            f"api={utc_win.api_start_yyyymmdd}..{utc_win.api_end_yyyymmdd}",
        )
    if utc_win.api_query_chunks() != [("20260813", "20260813")]:
        _fail("6b. utc day", f"chunks={utc_win.api_query_chunks()}")
    if utc_win.timezone_name != "UTC":
        _fail("6b. utc day", f"tz={utc_win.timezone_name}")
    print("PASS 6b. UTC calendar day start_date==end_date")

    if not p1.id.startswith("content_creators"):
        _fail("7. checkpoints", f"P1 id={p1.id}")
    ckpt_p1 = f"{p1.id}_{p1.resolve_handle_group_name(sample=False)}_{window.research_date}.json"
    ckpt_p2 = f"{p2.id}_{p2.resolve_handle_group_name(sample=False)}_{window.research_date}.json"
    ckpt_p3 = f"{p3.id}_{window.research_date}.json"
    if not ckpt_p1.startswith("content_creators_"):
        _fail("7. checkpoints", ckpt_p1)
    if not ckpt_p2.startswith("news_"):
        _fail("7. checkpoints", ckpt_p2)
    if ckpt_p3 != "keyword_2026-08-13.json":
        _fail("7. checkpoints", ckpt_p3)
    print("PASS 7. checkpoint namespaces")

    if p1.export_dir != "p1_content_creators/results/csv":
        _fail("8. CSV", p1.export_dir)
    if p2.export_dir != "p2_news/results/csv":
        _fail("8. CSV", p2.export_dir)
    if p3.export_dir != "p3_keywords/results/csv":
        _fail("8. CSV", p3.export_dir)
    print("PASS 8. CSV destinations")

    if p1.bigquery_table != "content_creators" or CONTENT_CREATORS_TABLE != "content_creators":
        _fail("9. BQ", f"P1 table={p1.bigquery_table}/{CONTENT_CREATORS_TABLE}")
    if p2.bigquery_table != "news" or NEWS_TABLE != "news":
        _fail("9. BQ", f"P2 table={p2.bigquery_table}/{NEWS_TABLE}")
    if p3.bigquery_table != "keyword" or KEYWORD_TABLE != "keyword":
        _fail("9. BQ", f"P3 table={p3.bigquery_table}/{KEYWORD_TABLE}")
    if ENRICHED_TABLE != "tiktok_video_enriched":
        _fail("9. BQ", f"v5 table={ENRICHED_TABLE}")
    print("PASS 9. BigQuery destinations")

    if p1.id != "content_creators" or p2.id != "news" or p3.id != "keyword":
        _fail("10. pipeline_id", f"{p1.id}/{p2.id}/{p3.id}")
    if get_pipeline(cfg, "news_accounts").id != "news":
        _fail("10. pipeline_id", "news_accounts alias failed")
    if get_pipeline(cfg, "keyword_search").id != "keyword":
        _fail("10. pipeline_id", "keyword_search alias failed")
    print("PASS 10. pipeline_id")

    if p1.resolved_api_source() != "CONTENT_CREATOR_API":
        _fail("11. api_source", p1.resolved_api_source())
    if p2.resolved_api_source() != "NEWS_API":
        _fail("11. api_source", p2.resolved_api_source())
    if p3.resolved_api_source() != "KEYWORD_SEARCH_API":
        _fail("11. api_source", p3.resolved_api_source())
    print("PASS 11. api_source")

    sets = p3.exclusion_handle_sets(cfg)
    if sets.get("pipeline_1") != p1_handles:
        _fail("12. P3 exclusion", "pipeline_1 set is not newsfluencer_combined.txt")
    if sets.get("pipeline_2") != p2_handles:
        _fail("12. P3 exclusion", "pipeline_2 set is not news_accounts.txt")
    overlap = set(p1_handles) & set(p2_handles)
    if overlap != P1_P2_OVERLAP:
        _fail("12. P3 exclusion", f"overlap={overlap}")
    if p3.exclude_handle_groups:
        _fail("12. P3 exclusion", f"YAML groups still configured: {p3.exclude_handle_groups}")
    if _classify_exclusion("underthedesknews", set(p1_handles), set(p2_handles)) != "pipeline_1":
        _fail("12. P3 exclusion", "P1 handle not excluded")
    if _classify_exclusion("6abcactionnews", set(p1_handles), set(p2_handles)) != "pipeline_2":
        _fail("12. P3 exclusion", "P2 handle not excluded")
    if _classify_exclusion("btnewsroom", set(p1_handles), set(p2_handles)) != "overlap":
        _fail("12. P3 exclusion", "overlap handle not classified")
    if _classify_exclusion("unrelated_user", set(p1_handles), set(p2_handles)) is not None:
        _fail("12. P3 exclusion", "unrelated user excluded")
    print("PASS 12. P3 account exclusion from handle files")

    if merge_matched_keywords(["news"], "trump") != ["news", "trump"]:
        _fail("13. matched_keywords", "list+str merge failed")
    if merge_matched_keywords('["news","trump"]', ["trump", "ice"]) != ["news", "trump", "ice"]:
        _fail("13. matched_keywords", "json merge failed")
    with tempfile.TemporaryDirectory() as tmp:
        conn = get_connection(os.path.join(tmp, "t.db"))
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
            "SELECT matched_keywords, pipeline_id FROM videos WHERE video_id='123'"
        ).fetchone()
        conn.close()
        if parse_matched_keywords(raw[0]) != ["news", "trump"]:
            _fail("13. matched_keywords", f"sqlite merge got {raw[0]!r}")
        if raw[1] != "keyword":
            _fail("13. matched_keywords", f"pipeline_id={raw[1]}")
    print("PASS 13. matched_keywords merging")

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
        _fail("14. HTTP 500", f"result={result}")
    if calls["n"] != 3:
        _fail("14. HTTP 500", f"called {calls['n']} times")
    with tempfile.TemporaryDirectory() as tmp:
        ckpt = CheckpointStore(os.path.join(tmp, "keyword_2026-08-13.json"))
        apply_chunk_checkpoint(ckpt, "news", "20260813", "20260814", result)
        if ckpt.is_done("news", "20260813", "20260814"):
            _fail("14. HTTP 500", "checkpointed as completed")
        if not ckpt.is_failed("news", "20260813", "20260814"):
            _fail("14. HTTP 500", "not checkpointed as failed")
    print("PASS 14. HTTP 500 retry 3x then failed, not completed")

    if abort_reason(RuntimeError("rate_limited HTTP 429")) != "rate_limited":
        _fail("15. 429", "keyword abort_reason missed 429")
    if abort_reason(RuntimeError("daily_quota_limit_exceeded")) != "daily_quota_limit_exceeded":
        _fail("15. 429", "keyword abort_reason missed quota")
    if _abort_reason(RuntimeError("rate_limited HTTP 429")) != "rate_limited":
        _fail("15. 429", "handle abort_reason missed 429")
    abort_result = {
        "videos": None,
        "abort": "rate_limited",
        "failed": False,
        "attempts": 1,
        "error": RuntimeError("rate_limited HTTP 429"),
    }
    with tempfile.TemporaryDirectory() as tmp:
        ckpt = CheckpointStore(os.path.join(tmp, "keyword_2026-08-13.json"))
        apply_chunk_checkpoint(ckpt, "trump", "20260813", "20260814", abort_result)
        if ckpt.is_done("trump", "20260813", "20260814") or ckpt.is_failed(
            "trump", "20260813", "20260814"
        ):
            _fail("15. 429", "429/quota must leave checkpoint unset")
    print("PASS 15. 429/quota stop without marking completed")

    ok, errors = validate_pipeline_row(
        {"video_id": "1", "creator_username": "a", "pipeline_id": "content_creators"}
    )
    if not ok:
        _fail("16. no duplicate video_id", f"P1 row invalid: {errors}")
    bad_p1, _ = validate_pipeline_row(
        {"video_id": "1", "creator_username": "a", "pipeline_id": "news"}
    )
    if bad_p1:
        _fail("16. no duplicate video_id", "P1 validator accepted news pipeline_id")
    spec = keyword_search_schema_spec()
    if spec["table"] != "keyword":
        _fail("16. no duplicate video_id", spec["table"])
    print("PASS 16. one video_id row identity / validators")

    from enrichment.validate_row import (
        COLLECTION_STATUS_API_FAILED,
        handle_fail_video_id,
    )

    fail_ok, fail_errs = validate_pipeline_row(
        {
            "video_id": handle_fail_video_id("2026-08-28", "auntiekilljoy"),
            "creator_username": "auntiekilljoy",
            "pipeline_id": "content_creators",
            "collection_source": "content_creators",
            "collection_status": COLLECTION_STATUS_API_FAILED,
        }
    )
    if not fail_ok:
        _fail("16b. handle fail row", fail_errs)
    p1_fields = {f["name"] for f in BQ_SCHEMAS[CONTENT_CREATORS_TABLE]}
    if "collection_status" not in p1_fields or "api_error_code" not in p1_fields:
        _fail("16b. handle fail row", "missing collection_status/api_error_code")
    print("PASS 16b. failed-handle stub rows")

    enrich_src = (ROOT / "common/scripts/enrich_pipeline.py").read_text(encoding="utf-8")
    tree = ast.parse(enrich_src)
    dispatch = enrich_src
    if "sync_content_creator_video" not in dispatch or "sync_news_account_video" not in dispatch:
        _fail("17. table isolation", "missing pipeline sync functions")
    if "sync_keyword_search_video" not in dispatch:
        _fail("17. table isolation", "missing keyword sync")
    if CONTENT_CREATORS_TABLE == NEWS_TABLE or NEWS_TABLE == KEYWORD_TABLE:
        _fail("17. table isolation", "table constants collide")
    p1_fields = {f["name"] for f in BQ_SCHEMAS[CONTENT_CREATORS_TABLE]}
    p2_fields = {f["name"] for f in BQ_SCHEMAS[NEWS_TABLE]}
    p3_fields = {f["name"] for f in BQ_SCHEMAS[KEYWORD_TABLE]}
    if "matched_keywords" in p1_fields or "matched_keywords" in p2_fields:
        _fail("17. table isolation", "matched_keywords leaked into P1/P2")
    if "matched_keywords" not in p3_fields:
        _fail("17. table isolation", "P3 missing matched_keywords")
    bad_news, _ = validate_news_account_row(
        {"video_id": "1", "creator_username": "a", "pipeline_id": "content_creators"}
    )
    if bad_news:
        _fail("17. table isolation", "news validator accepted P1 provenance")
    bad_kw, errs = validate_keyword_search_row(
        {
            "video_id": "1",
            "creator_username": "a",
            "pipeline_id": "news",
            "matched_keywords": ["news"],
        }
    )
    if bad_kw:
        _fail("17. table isolation", f"keyword validator accepted news provenance: {errs}")
    print("PASS 17. P1/P2/P3 cannot write each other's tables")

    if ENRICHED_TABLE != "tiktok_video_enriched":
        _fail("18. v5.0 default", ENRICHED_TABLE)
    if "--pipeline" not in enrich_src:
        _fail("18. v5.0 default", "enrich_pipeline missing --pipeline")
    if "Never writes tiktok_video_enriched" not in enrich_src and "never write tiktok_video_enriched" not in enrich_src.lower():
        if "Active pipelines never write tiktok_video_enriched" not in enrich_src:
            _fail("18. v5.0 default", "enrich_pipeline still allows omitted --pipeline BQ writes")
    pull = (ROOT / "archive/v5/scripts/pull_videos.py").read_text(encoding="utf-8")
    if "content_creators" in pull and "ensure_content_creators_table" in pull:
        _fail("18. v5.0 default", "pull_videos.py wired to P1 table")
    print("PASS 18. archived v5 pull_videos is isolated; active enrich requires --pipeline")

    try:
        listed = subprocess.check_output(
            ["git", "ls-files"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).splitlines()
    except (subprocess.CalledProcessError, FileNotFoundError):
        listed = []
    if ".env" in listed:
        _fail("19. no secrets", ".env is tracked")
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    for name in (
        "NEWS_API_CLIENT_KEY",
        "NEWS_API_CLIENT_SECRET",
        "KEYWORD_SEARCH_API_CLIENT_KEY",
        "KEYWORD_SEARCH_API_CLIENT_SECRET",
    ):
        if not re.search(rf"^{re.escape(name)}=$", example, re.M):
            _fail("19. no secrets", f".env.example must contain empty {name}=")
    secret_assign = re.compile(
        r"^(TIKTOK_CLIENT_KEY|TIKTOK_CLIENT_SECRET|"
        r"CONTENT_CREATOR_TIKTOK_CLIENT_KEY|CONTENT_CREATOR_TIKTOK_CLIENT_SECRET|"
        r"NEWS_API_CLIENT_KEY|NEWS_API_CLIENT_SECRET|"
        r"KEYWORD_SEARCH_API_CLIENT_KEY|KEYWORD_SEARCH_API_CLIENT_SECRET)"
        r"[ \t]*=[ \t]*(.+)$",
        re.M,
    )
    for rel in listed:
        if not rel or rel.startswith("data/"):
            continue
        path = ROOT / rel
        if not path.is_file():
            continue
        try:
            body = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for match in secret_assign.finditer(body):
            value = match.group(2).strip().strip("'\"")
            if not value or value in {".", "...", "changeme", "your_key_here"}:
                continue
            if value.startswith("${"):
                continue
            _fail("19. no secrets", f"non-empty {match.group(1)} in tracked {rel}")
    print("PASS 19. no secrets committed")
    _ = tree  # parsed enrich_pipeline to prove it is valid Python

    # 21. Pipeline 3 pagination: keep successful pages after HTTP 500
    from tiktok.collection.daily_keyword_pipeline import collect_keywords

    def _raw_video(term: str, n: int, create_time: int) -> dict:
        return {
            "id": f"kv-{term}-{n:04d}",
            "username": "staticpagetest",
            "create_time": create_time,
            "video_description": f"{term} {n}",
            "hashtag_names": [],
            "like_count": 1,
            "share_count": 0,
            "comment_count": 0,
            "favorites_count": 0,
            "video_duration": 1,
        }

    def _ok_page(term: str, n: int, create_time: int, *, has_more: bool, cursor: int) -> dict:
        return {
            "ok": True,
            "http_status": 200,
            "videos": [_raw_video(term, n, create_time)],
            "has_more": has_more,
            "cursor": cursor,
            "search_id": "sid-static",
            "error_code": "ok",
            "error_message": "",
            "log_id": f"OK{n}",
        }

    def _err_page(cursor: int, search_id: str = "sid-static") -> dict:
        return {
            "ok": False,
            "http_status": 500,
            "videos": [],
            "has_more": False,
            "cursor": cursor,
            "search_id": search_id,
            "error_code": "internal_error",
            "error_message": "Something is wrong. Please try again later.",
            "log_id": "LOG500PARTIAL",
        }

    window21 = research_window("2026-08-13")
    create_ts = int(window21.start_utc.timestamp()) + 3600

    page_calls: List[Any] = []

    def _hundred_then_500(client, keyword, chunk_start, chunk_end, **kwargs):
        cursor = int(kwargs.get("cursor") or 0)
        page_calls.append((keyword, cursor))
        page_num = 0 if cursor == 0 else cursor // 100
        if page_num >= 100:
            return _err_page(cursor)
        return _ok_page(
            keyword, page_num, create_ts, has_more=True, cursor=(page_num + 1) * 100
        )

    persisted: List[str] = []

    def _capture_page(videos, meta):
        persisted.extend(v["video_id"] for v in videos)

    page_calls.clear()
    result = query_keyword_chunk_with_retries(
        client=None,
        keyword="trump",
        chunk_start="20260813",
        chunk_end="20260815",
        retry_attempts=3,
        sleep_seconds=0,
        query_fn=_hundred_then_500,
        on_page=_capture_page,
    )
    if not result.get("partial") or result.get("failed") or result.get("abort"):
        _fail("21. partial pagination", f"expected partial, got { {k: result.get(k) for k in ('partial','failed','abort','page','cursor')} }")
    if len(result.get("videos") or []) != 100:
        _fail("21. partial pagination", f"kept {len(result.get('videos') or [])} videos, expected 100")
    if len(persisted) != 100:
        _fail("21. partial pagination", f"on_page saw {len(persisted)} videos")
    if result.get("page") != 100:
        _fail("21. partial pagination", f"page={result.get('page')}")
    if int(result.get("cursor") or 0) != 10000:
        _fail("21. partial pagination", f"failing cursor={result.get('cursor')}")
    if result.get("log_id") != "LOG500PARTIAL":
        _fail("21. partial pagination", f"log_id={result.get('log_id')}")
    fail_calls = [c for c in page_calls if c[1] == 10000]
    if len(fail_calls) != 3:
        _fail("21. partial pagination", f"failing page retried {len(fail_calls)} times, expected 3")

    with tempfile.TemporaryDirectory() as tmp:
        ckpt = CheckpointStore(os.path.join(tmp, "keyword_2026-08-13.json"))
        apply_chunk_checkpoint(ckpt, "trump", "20260813", "20260815", result)
        if ckpt.is_done("trump", "20260813", "20260815"):
            _fail("21. partial pagination", "partial keyword marked completed")
        if ckpt.is_failed("trump", "20260813", "20260815"):
            _fail("21. partial pagination", "partial keyword marked failed")
        if not ckpt.is_partial("trump", "20260813", "20260815"):
            _fail("21. partial pagination", "partial keyword not recorded")
        if ckpt.is_settled("trump", "20260813", "20260815"):
            _fail("21. partial pagination", "partial must remain pending for resume")
        meta = ckpt.get_partial("trump", "20260813", "20260815") or {}
        if int(meta.get("cursor") or 0) != 10000 or int(meta.get("page") or 0) != 100:
            _fail("21. partial pagination", f"resume meta={meta}")
        saved = json.loads(Path(ckpt.filepath).read_text(encoding="utf-8"))
        if "partial" not in saved or "trump|20260813|20260815" not in saved["partial"]:
            _fail("21. partial pagination", "partial key missing from checkpoint file")

        resume_calls: List[Any] = []

        def _resume_fn(client, keyword, chunk_start, chunk_end, **kwargs):
            cursor = int(kwargs.get("cursor") or 0)
            resume_calls.append((cursor, kwargs.get("search_id")))
            if cursor < 10000:
                _fail("21. partial pagination", f"resume re-fetched cursor={cursor}")
            if kwargs.get("search_id") != "sid-static":
                _fail("21. partial pagination", f"resume search_id={kwargs.get('search_id')}")
            return {
                "ok": True,
                "http_status": 200,
                "videos": [_raw_video(keyword, 100, create_ts)],
                "has_more": False,
                "cursor": 10100,
                "search_id": "sid-static",
                "error_code": "ok",
                "error_message": "",
                "log_id": "OKRESUME",
            }

        resume_result = query_keyword_chunk_with_retries(
            client=None,
            keyword="trump",
            chunk_start="20260813",
            chunk_end="20260815",
            retry_attempts=3,
            sleep_seconds=0,
            query_fn=_resume_fn,
            resume=ckpt.get_partial("trump", "20260813", "20260815"),
        )
        if resume_result.get("failed") or resume_result.get("partial"):
            _fail("21. partial pagination", f"resume result={resume_result.get('partial')}")
        if resume_calls != [(10000, "sid-static")]:
            _fail("21. partial pagination", f"resume cursors={resume_calls}")
        apply_chunk_checkpoint(ckpt, "trump", "20260813", "20260815", resume_result)
        if not ckpt.is_done("trump", "20260813", "20260815") or ckpt.is_partial(
            "trump", "20260813", "20260815"
        ):
            _fail("21. partial pagination", "resume did not mark completed")

        p1_ckpt = CheckpointStore(os.path.join(tmp, "content_creators_dummy.json"))
        p1_ckpt.mark_done("alice", "20260813", "20260815")
        p1_disk = json.loads(Path(p1_ckpt.filepath).read_text(encoding="utf-8"))
        if "partial" in p1_disk:
            _fail("21. partial pagination", "P1 checkpoint file unexpectedly contains partial")

    os.environ["KEYWORD_SEARCH_API_CLIENT_KEY"] = "p3-static-key"
    os.environ["KEYWORD_SEARCH_API_CLIENT_SECRET"] = "p3-static-secret"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            cfg21 = load_config("common/config.yaml")
            cfg21.paths = dict(cfg21.paths)
            cfg21.paths["database"] = os.path.join(tmp, "t.db")
            cfg21.paths["checkpoints"] = os.path.join(tmp, "ck")
            cfg21.paths["exports"] = os.path.join(tmp, "ex")
            cfg21.paths["raw_responses"] = os.path.join(tmp, "raw")
            os.makedirs(cfg21.paths["checkpoints"], exist_ok=True)
            os.makedirs(cfg21.paths["exports"], exist_ok=True)

            seen_kw: List[str] = []

            def _two_keywords(client, keyword, chunk_start, chunk_end, **kwargs):
                cursor = int(kwargs.get("cursor") or 0)
                seen_kw.append(keyword)
                if keyword == "trump":
                    if cursor == 0:
                        return _ok_page("trump", 0, create_ts, has_more=True, cursor=100)
                    return _err_page(cursor)
                if keyword == "ice":
                    return _ok_page("ice", 0, create_ts, has_more=False, cursor=100)
                _fail("21. partial pagination", f"unexpected keyword {keyword}")
                return _err_page(cursor)

            with patch(
                "tiktok.collection.daily_keyword_pipeline.auth.init",
                lambda *a, **k: None,
            ):
                stats = collect_keywords(
                    cfg=cfg21,
                    pipeline=p3,
                    keywords=["trump", "ice"],
                    window=window21,
                    reset_checkpoints=True,
                    query_fn=_two_keywords,
                    sleep_seconds=0,
                )
            if stats.get("stop_reason"):
                _fail("21. partial pagination", f"run stopped: {stats.get('stop_reason')}")
            if seen_kw.count("ice") < 1:
                _fail("21. partial pagination", f"later keyword did not run: {seen_kw}")
            if stats.get("keywords_partial") != 1:
                _fail("21. partial pagination", f"keywords_partial={stats.get('keywords_partial')}")
            if stats.get("keywords_query_ok") != 1:
                _fail("21. partial pagination", f"keywords_query_ok={stats.get('keywords_query_ok')}")
            collected = list(stats.get("collected_video_ids") or [])
            if "kv-trump-0000" not in collected or "kv-ice-0000" not in collected:
                _fail(
                    "21. partial pagination",
                    f"partial videos not in collected_video_ids={collected}",
                )
            conn = get_connection(cfg21.paths["database"])
            n_rows = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
            if int(n_rows) != 2:
                _fail("21. partial pagination", f"sqlite rows={n_rows}, expected 2 (partial+complete)")
            conn.close()

            seen_quota: List[str] = []

            def _quota_after_page(client, keyword, chunk_start, chunk_end, **kwargs):
                seen_quota.append(keyword)
                if keyword == "war":
                    cursor = int(kwargs.get("cursor") or 0)
                    if cursor == 0:
                        return _ok_page("war", 0, create_ts, has_more=True, cursor=100)
                    raise RuntimeError("daily_quota_limit_exceeded")
                _fail("21. partial pagination", "quota stop allowed a later keyword")
                return _err_page(0)

            with patch(
                "tiktok.collection.daily_keyword_pipeline.auth.init",
                lambda *a, **k: None,
            ):
                quota_stats = collect_keywords(
                    cfg=cfg21,
                    pipeline=p3,
                    keywords=["war", "tsa"],
                    window=window21,
                    reset_checkpoints=True,
                    query_fn=_quota_after_page,
                    sleep_seconds=0,
                )
            if quota_stats.get("stop_reason") != "daily_quota_limit_exceeded":
                _fail("21. partial pagination", f"quota stop_reason={quota_stats.get('stop_reason')}")
            if "tsa" in seen_quota:
                _fail("21. partial pagination", "quota did not stop before the next keyword")
            qckpt = CheckpointStore(
                os.path.join(cfg21.paths["checkpoints"], "keyword_2026-08-13.json")
            )
            if not qckpt.is_partial("war", window21.api_start_yyyymmdd, window21.api_end_yyyymmdd):
                _fail("21. partial pagination", "quota mid-keyword should record partial")
            if qckpt.is_done("war", window21.api_start_yyyymmdd, window21.api_end_yyyymmdd):
                _fail("21. partial pagination", "quota keyword marked completed")

            conn = get_connection(cfg21.paths["database"])
            upsert_collected_video(
                conn,
                {
                    "video_id": "merge-1",
                    "username": "staticpagetest",
                    "video_url": "https://www.tiktok.com/@staticpagetest/video/merge-1",
                    "create_time": create_ts,
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
                    "matched_keyword": "news",
                },
            )
            upsert_collected_video(
                conn,
                {
                    "video_id": "merge-1",
                    "username": "staticpagetest",
                    "video_url": "https://www.tiktok.com/@staticpagetest/video/merge-1",
                    "create_time": create_ts,
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
                    "matched_keyword": "trump",
                },
            )
            merged = conn.execute(
                "SELECT matched_keywords FROM videos WHERE video_id='merge-1'"
            ).fetchone()[0]
            conn.close()
            if parse_matched_keywords(merged) != ["news", "trump"]:
                _fail("21. partial pagination", f"matched_keywords merge={merged!r}")
    finally:
        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_KEY", None)
        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_SECRET", None)

    handle_src = (ROOT / "common/tiktok/collection/daily_handle_pipeline.py").read_text(
        encoding="utf-8"
    )
    if "mark_partial" in handle_src or "fetch_keyword_query_page" in handle_src:
        _fail("21. partial pagination", "Pipeline 1/2 handle collector picked up P3 pagination")
    news_src = (ROOT / "p2_news/scripts/run_news.py").read_text(encoding="utf-8")
    p1_src = (ROOT / "p1_content_creators/scripts/run_content_creators.py").read_text(encoding="utf-8")
    if "fetch_keyword_query_page" in news_src or "fetch_keyword_query_page" in p1_src:
        _fail("21. partial pagination", "P1/P2 runners imported keyword page fetch")
    print("PASS 21. P3 keeps successful pages on HTTP 500; P1/P2 unchanged")

    # 22. End-to-end P3: enrichment handoff, BQ isolation, abort-on-auth/429
    from enrichment.bigquery_loader import (
        DEFAULT_BQ_DATASET,
        DEFAULT_GCP_PROJECT,
        keyword_search_table_id,
        sync_keyword_search_video,
    )

    def _abort_fn(message: str):
        def _fn(client, keyword, chunk_start, chunk_end, **kwargs):
            raise RuntimeError(message)

        return _fn

    for abort_msg, abort_code in (
        ("rate_limited HTTP 429", "rate_limited"),
        ("daily_quota_limit_exceeded", "daily_quota_limit_exceeded"),
        ("authentication_failure HTTP 401", "authentication_failure"),
        ("authentication_failure HTTP 403", "authentication_failure"),
    ):
        abort_result = query_keyword_chunk_with_retries(
            client=None,
            keyword="tsa",
            chunk_start="20260813",
            chunk_end="20260815",
            retry_attempts=3,
            sleep_seconds=0,
            query_fn=_abort_fn(abort_msg),
        )
        if abort_result.get("abort") != abort_code:
            _fail("22. P3 e2e", f"{abort_msg} abort={abort_result.get('abort')}")
        if abort_result.get("failed") or abort_result.get("partial"):
            _fail("22. P3 e2e", f"{abort_msg} should not mark failed/partial with 0 pages")
        with tempfile.TemporaryDirectory() as tmp:
            ckpt = CheckpointStore(os.path.join(tmp, "keyword_abort.json"))
            apply_chunk_checkpoint(ckpt, "tsa", "20260813", "20260815", abort_result)
            if (
                ckpt.is_done("tsa", "20260813", "20260815")
                or ckpt.is_failed("tsa", "20260813", "20260815")
                or ckpt.is_partial("tsa", "20260813", "20260815")
            ):
                _fail("22. P3 e2e", f"{abort_msg} mutated checkpoint with 0 pages")

    run_kw = (ROOT / "p3_keywords/scripts/run_keyword.py").read_text(encoding="utf-8")
    if "--pipeline" not in run_kw or "PIPELINE_KEYWORD" not in run_kw:
        _fail("22. P3 e2e", "run_keyword.py does not pass --pipeline keyword to enrich")
    if "common/scripts/enrich_pipeline.py" not in run_kw:
        _fail("22. P3 e2e", "run_keyword.py does not call enrich_pipeline.py")
    if "collected_video_ids" not in run_kw:
        _fail("22. P3 e2e", "run_keyword.py does not enrich collected_video_ids")
    if "--steps" in run_kw and "ocr,emoji" in run_kw and "skip_whisper" not in run_kw:
        _fail("22. P3 e2e", "Whisper skip is unconditional")
    enrich_src22 = (ROOT / "common/scripts/enrich_pipeline.py").read_text(encoding="utf-8")
    if 'default="transcript,ocr,emoji"' not in enrich_src22:
        _fail("22. P3 e2e", "enrich_pipeline default steps lost Whisper/OCR/emoji")
    if "scripts/transcription_worker.py" not in enrich_src22:
        _fail("22. P3 e2e", "Whisper worker not in enrich_pipeline")
    if "scripts/ocr_worker.py" not in enrich_src22:
        _fail("22. P3 e2e", "OCR worker not in enrich_pipeline")
    if "scripts/emoji_worker.py" not in enrich_src22:
        _fail("22. P3 e2e", "emoji worker not in enrich_pipeline")
    kw_branch = enrich_src22.split('elif pipeline_id == "keyword":', 1)
    if len(kw_branch) != 2:
        _fail("22. P3 e2e", "missing keyword BQ branch")
    kw_branch = kw_branch[1].split("else:", 1)[0]
    if "sync_keyword_search_video" not in kw_branch:
        _fail("22. P3 e2e", "keyword BQ branch does not call sync_keyword_search_video")
    if "sync_content_creator_video" in kw_branch or "sync_news_account_video" in kw_branch:
        _fail("22. P3 e2e", "keyword BQ branch writes P1/P2 tables")
    if "sync_video_from_sqlite" in kw_branch:
        _fail("22. P3 e2e", "keyword BQ branch writes tiktok_video_enriched")

    import inspect as _inspect

    kw_sync_src = _inspect.getsource(sync_keyword_search_video)
    if "ENRICHED_TABLE" in kw_sync_src or "sync_video_from_sqlite" in kw_sync_src:
        _fail("22. P3 e2e", "sync_keyword_search_video writes v5 enriched table")
    if "CONTENT_CREATORS_TABLE" in kw_sync_src or "NEWS_TABLE" in kw_sync_src:
        _fail("22. P3 e2e", "sync_keyword_search_video writes P1/P2 tables")
    if DEFAULT_GCP_PROJECT != "cfme-mediaengagment-prod":
        _fail("22. P3 e2e", DEFAULT_GCP_PROJECT)
    if DEFAULT_BQ_DATASET != "tiktok_research" or KEYWORD_TABLE != "keyword":
        _fail("22. P3 e2e", f"{DEFAULT_BQ_DATASET}.{KEYWORD_TABLE}")
    saved_proj = os.environ.get("BIGQUERY_PROJECT")
    saved_gcp = os.environ.get("GCP_PROJECT")
    saved_ds = os.environ.get("BIGQUERY_DATASET")
    os.environ.pop("BIGQUERY_PROJECT", None)
    os.environ.pop("GCP_PROJECT", None)
    os.environ.pop("BIGQUERY_DATASET", None)
    try:
        table_id = keyword_search_table_id()
    finally:
        if saved_proj is None:
            os.environ.pop("BIGQUERY_PROJECT", None)
        else:
            os.environ["BIGQUERY_PROJECT"] = saved_proj
        if saved_gcp is None:
            os.environ.pop("GCP_PROJECT", None)
        else:
            os.environ["GCP_PROJECT"] = saved_gcp
        if saved_ds is None:
            os.environ.pop("BIGQUERY_DATASET", None)
        else:
            os.environ["BIGQUERY_DATASET"] = saved_ds
    if table_id != "cfme-mediaengagment-prod.tiktok_research.keyword":
        _fail("22. P3 e2e", f"keyword table id={table_id}")

    if "PIPELINE_CONTENT_CREATORS" not in p1_src or "PIPELINE_KEYWORD" in p1_src:
        _fail("22. P3 e2e", "P1 runner pipeline wiring changed")
    if "PIPELINE_NEWS" not in news_src or "PIPELINE_KEYWORD" in news_src:
        _fail("22. P3 e2e", "P2 runner pipeline wiring changed")
    if "query_videos_for_chunk" not in handle_src:
        _fail("22. P3 e2e", "P1/P2 username collector no longer uses query_videos_for_chunk")
    if "is_partial" in handle_src:
        _fail("22. P3 e2e", "P1/P2 collector now treats partial checkpoints")
    print("PASS 22. P3 enrichment/BQ isolation and abort-on-429/auth")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as e:
        print(f"FAIL {e}", file=sys.stderr)
        raise SystemExit(1)
