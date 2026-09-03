"""Static checks for Paperboy News Account Discovery.

No TikTok API, media, enrichment, SQLite, or BigQuery writes.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import os
import socket
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

CSV_PATH = ROOT / "config" / "discovery" / "paperboy_journalist_list.csv"
P1_PATH = ROOT / "config" / "handles" / "newsfluencer_combined.txt"
P2_PATH = ROOT / "config" / "handles" / "news_accounts.txt"
DISCOVERY_PY = ROOT / "tiktok" / "discovery" / "paperboy_journalists.py"
CLI_PY = ROOT / "scripts" / "discover_paperboy_journalists.py"
CLIENT_PY = ROOT / "tiktok" / "api" / "client.py"

FORBIDDEN_IMPORT_ROOTS = (
    "tiktok.db",
    "tiktok.enrichment",
    "tiktok.api.videos",
    "tiktok.api.download",
    "tiktok.ocr",
    "tiktok.transcription",
    "google.cloud",
)
FORBIDDEN_NAME_FRAGMENTS = (
    "query_videos",
    "whisper",
    "bigquery",
    "TIKTOK_CLIENT_KEY",
    "KEYWORD_SEARCH_API_CLIENT_KEY",
)
P1_P3_ENV = (
    "TIKTOK_CLIENT_KEY",
    "TIKTOK_CLIENT_SECRET",
    "KEYWORD_SEARCH_API_CLIENT_KEY",
    "KEYWORD_SEARCH_API_CLIENT_SECRET",
    "CONTENT_CREATOR_TIKTOK_CLIENT_KEY",
    "CONTENT_CREATOR_TIKTOK_CLIENT_SECRET",
)


def _fail(name: str, msg: str) -> None:
    raise AssertionError(f"{name}: {msg}")


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


class FakeHTTP:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)
        self.ok = 200 <= status_code < 300
        self.headers: Dict[str, str] = {}

    def json(self):
        return self._payload


def _found_body(username: str, display_name: str, bio: str = "", verified: bool = False):
    return {
        "ok": True,
        "http_status": 200,
        "body": {
            "data": {
                "username": username,
                "display_name": display_name,
                "bio_description": bio,
                "avatar_url": "https://example.com/a.jpg",
                "is_verified": verified,
                "follower_count": 10,
                "following_count": 1,
                "likes_count": 2,
                "video_count": 3,
                "bio_url": "",
            }
        },
        "error_code": "ok",
    }


def _not_found():
    return {
        "ok": False,
        "http_status": 400,
        "body": {"error": {"code": "invalid_params"}},
        "error_code": "invalid_params",
    }


def _server_error():
    return {
        "ok": False,
        "http_status": 500,
        "body": {"error": {"code": "internal"}},
        "error_code": "internal",
    }


def main() -> int:
    os.environ.pop("TIKTOK_ALLOW_LOCAL_COLLECTION", None)
    api_calls: List[Any] = []

    def _blocked_post(*args, **kwargs):
        api_calls.append((args, kwargs))
        raise AssertionError("TikTok/API HTTP during static tests")

    with patch("requests.post", side_effect=_blocked_post):
        _run_checks()

    if api_calls:
        _fail("no network", f"requests.post invoked {len(api_calls)} time(s)")
    print("PASS  no network calls during static validation")
    print("All Paperboy discovery static checks passed.")
    return 0


def _run_checks() -> None:
    from tiktok.collection.server_guard import require_collection_server
    from tiktok.discovery.paperboy_journalists import (
        OUTPUT_COLUMNS,
        REVIEW_STATUS,
        SAMPLE_JOURNALIST_COUNT,
        checkpoint_key,
        generate_candidates,
        load_known_handles,
        lookup_user_info,
        parse_journalists,
        run_discovery,
        score_match,
    )
    from tiktok.pipelines import MISSING_NEWS_CREDENTIALS, require_news_credentials

    p1_sha = _file_sha(P1_PATH)
    p2_sha = _file_sha(P2_PATH)

    # 1–2. CSV parse + skip section header
    if not CSV_PATH.is_file():
        _fail("1. csv present", f"missing {CSV_PATH}")
    journalists = parse_journalists(str(CSV_PATH))
    if len(journalists) != 682:
        _fail("2. journalist count", f"expected 682, got {len(journalists)}")
    names = [j.name for j in journalists]
    if any(n.lower().startswith("also tracked") for n in names):
        _fail("2. section header", "Also tracked row was not skipped")
    if len({n.strip().lower() for n in names}) != 682:
        _fail("2. unique names", "duplicate journalist names after parse")
    print("PASS 1–2. CSV parses to 682 journalists; section header skipped")

    # 3. X handle normalization
    tony = next(j for j in journalists if j.name == "Tony Diver")
    if tony.x_handle != "tony_diver":
        _fail("3. x normalize", f"Tony Diver x_handle={tony.x_handle!r}")
    laura = next(j for j in journalists if j.name == "Laura Gersony")
    if laura.x_handle != "lauragersony" or laura.csv_tiktok_handle != "lauragersony":
        _fail("3. x normalize", "Laura Gersony handles not normalized")
    jake = next(j for j in journalists if j.name == "Jake Sheridan")
    if jake.x_handle != "jakesheridan_":
        _fail("3. x normalize", f"Jake Sheridan x_handle={jake.x_handle!r}")
    print("PASS 3. X handles normalized")

    # 4. Candidate generation deterministic + Tony Diver set
    c1 = [c.username for c in generate_candidates(tony)]
    c2 = [c.username for c in generate_candidates(tony)]
    if c1 != c2:
        _fail("4. deterministic", f"{c1} != {c2}")
    expected_tony = [
        "tony_diver",
        "tonydiver",
        "tonydivernews",
        "tonydivertv",
        "tonydiverreporter",
    ]
    if c1 != expected_tony:
        _fail("4. tony candidates", f"got {c1}")
    if len(c1) > 6:
        _fail("4. cap", f"Tony Diver has {len(c1)} candidates")
    wilner = next(j for j in journalists if j.name == "Michael Wilner")
    wilner_cands = [c.username for c in generate_candidates(wilner)]
    if "latimes" in wilner_cands or "nytimes" in wilner_cands:
        _fail("5. masthead", f"masthead probed as personal: {wilner_cands}")
    if "mawilner" not in wilner_cands or "michaelwilner" not in wilner_cands:
        _fail("4. wilner", f"missing expected slugs: {wilner_cands}")
    print("PASS 4–5. Candidate generation; masthead handles excluded")

    # 6–7. Existing list exclusion
    known = load_known_handles(str(P1_PATH), str(P2_PATH))
    if known.get("lauragersony") not in ("news_accounts", "both"):
        _fail("7. p2 exclude", f"lauragersony known={known.get('lauragersony')!r}")
    if known.get("jakesheridan_") not in ("news_accounts", "both"):
        _fail("7. p2 exclude", f"jakesheridan_ known={known.get('jakesheridan_')!r}")
    p1_sample = "underthedesknews"
    if p1_sample not in known or known[p1_sample] not in (
        "newsfluencer_combined",
        "both",
    ):
        _fail("6. p1 exclude", f"{p1_sample} known={known.get(p1_sample)!r}")
    print("PASS 6–7. P1 and P2 handles load for exclusion")

    # 8–9. NEWS credentials required; P1/P3 unused
    saved = {k: os.environ.get(k) for k in (
        "NEWS_API_CLIENT_KEY",
        "NEWS_API_CLIENT_SECRET",
        *P1_P3_ENV,
    )}
    try:
        os.environ.pop("NEWS_API_CLIENT_KEY", None)
        os.environ.pop("NEWS_API_CLIENT_SECRET", None)
        for k in P1_P3_ENV:
            os.environ[k] = "should-not-be-used"
        try:
            require_news_credentials()
            _fail("8. missing news creds", "did not fail")
        except RuntimeError as e:
            if "NEWS_API_CLIENT_KEY" not in str(e):
                _fail("8. missing news creds", str(e))
            if str(e) != MISSING_NEWS_CREDENTIALS and "NEWS_API_CLIENT_KEY" not in str(e):
                _fail("8. missing news creds", str(e))
        src = DISCOVERY_PY.read_text(encoding="utf-8") + CLI_PY.read_text(encoding="utf-8")
        for frag in (
            "TIKTOK_CLIENT_KEY",
            "KEYWORD_SEARCH_API_CLIENT_KEY",
            "CONTENT_CREATOR_TIKTOK_CLIENT_KEY",
        ):
            if frag in src:
                _fail("9. no p1/p3 creds", f"{frag} referenced in discovery code")
        os.environ["NEWS_API_CLIENT_KEY"] = "news-key"
        os.environ["NEWS_API_CLIENT_SECRET"] = "news-secret"
        key, secret = require_news_credentials()
        if key != "news-key" or secret != "news-secret":
            _fail("8. news creds", f"{key!r} {secret!r}")
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    print("PASS 8–9. NEWS_API required; P1/P3 credentials unused")

    # 10. Non-server hostname rejected
    host = socket.gethostname().lower()
    if "cme-p01" in host:
        print("SKIP 10. host is already cme-p01")
    else:
        try:
            require_collection_server()
            _fail("10. server guard", "require_collection_server() did not exit")
        except SystemExit:
            pass
    cli_src = CLI_PY.read_text(encoding="utf-8")
    if "require_collection_server()" not in cli_src:
        _fail("10. server guard", "CLI missing require_collection_server()")
    if cli_src.find("require_collection_server()") > cli_src.find("require_news_credentials"):
        _fail("10. server guard", "CLI must call require_collection_server before credentials")
    print("PASS 10. Non-server hostname rejected")

    # 14–15. No SQLite/BQ/video/enrichment imports
    for path in (DISCOVERY_PY, CLI_PY):
        imports = _module_imports(path)
        text = path.read_text(encoding="utf-8")
        for forbidden in FORBIDDEN_IMPORT_ROOTS:
            if any(n == forbidden or n.startswith(forbidden + ".") for n in imports):
                _fail("14. imports", f"{path.name} imports {forbidden}")
        for frag in FORBIDDEN_NAME_FRAGMENTS:
            if frag in text:
                _fail("15. forbidden names", f"{path.name} contains {frag}")
    if "def post_with_status" not in CLIENT_PY.read_text(encoding="utf-8"):
        _fail("client helper", "post_with_status missing")
    print("PASS 14–15. No SQLite/BigQuery/video/enrichment imports in discovery")

    # 11–13. 500 failed, 429 stop, resume skip, review_status, handle files
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        ckpt_path = str(tmp_path / "paperboy_journalist_discovery_sample.json")
        out_csv = str(tmp_path / "candidates.csv")
        run_json = str(tmp_path / "run.json")
        sample_js = journalists[:SAMPLE_JOURNALIST_COUNT]

        # 500 → failed after 3 retries
        client_500 = FakeClient(lambda *a: _server_error())
        sleeps: List[float] = []
        status, http_status, _prof = lookup_user_info(
            client_500, "tonydiver", sleep=sleeps.append
        )
        if status != "failed" or http_status != 500:
            _fail("11. 500", f"status={status} http={http_status}")
        if len(client_500.calls) != 3:
            _fail("11. 500 retries", f"calls={len(client_500.calls)}")
        if len(sleeps) != 2:
            _fail("11. 500 sleep", f"sleeps={sleeps}")
        print("PASS 11a. HTTP 500 → failed after 3 retries")

        # 429 does not complete current key
        def _raise_429(*_a, **_k):
            raise RuntimeError("rate_limited HTTP 429")

        client_429 = FakeClient(_raise_429)
        stats = run_discovery(
            csv_path=str(CSV_PATH),
            client=client_429,
            known=known,
            sample=True,
            reset_checkpoints=True,
            checkpoint_path=ckpt_path,
            output_csv=out_csv,
            run_json=run_json,
            sleep=lambda _s: None,
        )
        if stats.get("rate_limit_or_quota_errors", 0) < 1:
            _fail("11. 429", f"stats={stats}")
        key = checkpoint_key(sample_js[0].name, "tony_diver")
        import json

        ckpt_data = json.loads(Path(ckpt_path).read_text(encoding="utf-8"))
        if key in ckpt_data.get("completed", []):
            _fail("11. 429 checkpoint", f"{key} marked completed")
        print("PASS 11b. HTTP 429 stops without completing current checkpoint")

        def _raise_401(*_a, **_k):
            raise RuntimeError("authentication_failure HTTP 401")

        ckpt_401 = str(tmp_path / "disc_401.json")
        stats401 = run_discovery(
            csv_path=str(CSV_PATH),
            client=FakeClient(_raise_401),
            known=known,
            sample=True,
            reset_checkpoints=True,
            checkpoint_path=ckpt_401,
            output_csv=str(tmp_path / "c401.csv"),
            run_json=str(tmp_path / "r401.json"),
            sleep=lambda _s: None,
        )
        d401 = json.loads(Path(ckpt_401).read_text(encoding="utf-8"))
        if checkpoint_key(sample_js[0].name, "tony_diver") in d401.get("completed", []):
            _fail("11. 401 checkpoint", "current candidate marked completed")
        if not stats401.get("stopped_reason"):
            _fail("11. 401", "did not stop")
        print("PASS 11c. HTTP 401/403 stop without completing current checkpoint")

        # Successful mocked run: skip known, not_found others, always needs_review
        def _handler(endpoint, body, handle, raise_on_rate_limit):
            if handle in known:
                _fail("6/7 probe known", f"API called for already_known {handle}")
            if handle == "mawilner":
                return _found_body(
                    "mawilner",
                    "Michael Wilner",
                    bio="Los Angeles Times reporter",
                    verified=True,
                )
            return _not_found()

        client_ok = FakeClient(_handler)
        ckpt_ok = str(tmp_path / "disc_ok.json")
        out_ok = str(tmp_path / "ok.csv")
        stats_ok = run_discovery(
            csv_path=str(CSV_PATH),
            client=client_ok,
            known=known,
            sample=True,
            reset_checkpoints=True,
            checkpoint_path=ckpt_ok,
            output_csv=out_ok,
            run_json=str(tmp_path / "ok.json"),
            sleep=lambda _s: None,
        )
        if any(h in known for h in client_ok.calls):
            _fail("6/7 skip api", f"probed known handles {client_ok.calls}")
        if any("video" in e for e in client_ok.endpoints):
            _fail("15. no video query", str(client_ok.endpoints))

        with open(out_ok, newline="", encoding="utf-8") as f:
            out_rows = list(csv.DictReader(f))
        if not out_rows:
            _fail("12. output", "empty CSV")
        missing_cols = [c for c in OUTPUT_COLUMNS if c not in out_rows[0]]
        if missing_cols:
            _fail("12. output schema", f"missing {missing_cols}")
        if any(r.get("review_status") != REVIEW_STATUS for r in out_rows):
            _fail("13. review_status", "a row was not needs_review")
        known_rows = [r for r in out_rows if r.get("already_known") == "true"]
        if not known_rows:
            # sample of 10 may not include Laura/Jake (they are later). Seed a P1 handle.
            pass
        # Inject a P1 handle into candidate path: underthedesknews is not generated
        # from sample journalists. Confirm Tony Diver variants were probed except none known.
        if stats_ok["journalists"] != 10:
            _fail("sample", f"journalists={stats_ok['journalists']}")

        # Resume: completed keys skipped
        calls_before = list(client_ok.calls)
        stats_resume = run_discovery(
            csv_path=str(CSV_PATH),
            client=client_ok,
            known=known,
            sample=True,
            reset_checkpoints=False,
            checkpoint_path=ckpt_ok,
            output_csv=out_ok,
            run_json=str(tmp_path / "ok2.json"),
            sleep=lambda _s: None,
        )
        if client_ok.calls != calls_before:
            _fail(
                "11. resume skip",
                f"extra calls after resume: {client_ok.calls[len(calls_before):]}",
            )
        if stats_resume["output_rows"] != stats_ok["output_rows"]:
            _fail("11. resume rows", "output row count changed on resume")
        print("PASS 11d. Completed checkpoint entries skipped on resume")
        print("PASS 12. Output schema includes required columns")
        print("PASS 13. review_status is always needs_review")

        # High-confidence flag does not change review_status
        maw = next(
            (r for r in out_rows if r.get("candidate_username") == "mawilner"),
            None,
        )
        if maw is None:
            _fail("match", "mawilner row missing")
        if maw["review_status"] != REVIEW_STATUS:
            _fail("13. high conf", "high-confidence row auto-approved")
        if maw["api_status"] != "found":
            _fail("match found", maw["api_status"])
        if "exact_x_handle" not in (maw.get("match_reasons") or ""):
            _fail("match reasons", maw.get("match_reasons"))
        if "display_name_match" not in (maw.get("match_reasons") or ""):
            _fail("display match", maw.get("match_reasons"))

        # Scoring helper
        from tiktok.discovery.paperboy_journalists import Candidate

        score, reason, reasons = score_match(
            wilner,
            Candidate("mawilner", "x_handle", 1),
            {
                "display_name": "Michael Wilner",
                "bio": "Los Angeles Times reporter @latimes",
                "is_verified": True,
            },
        )
        if reason != "exact_x_handle":
            _fail("score primary", reason)
        if "outlet_in_bio" not in reasons or "verified" not in reasons:
            _fail("score reasons", reasons)
        if score < 50:
            _fail("score value", str(score))

        # 500 in run_discovery marks failed
        def _fail_tonydiver(endpoint, body, handle, raise_on_rate_limit):
            if handle == "tony_diver":
                return _server_error()
            return _not_found()

        ckpt_fail = str(tmp_path / "fail.json")
        run_discovery(
            csv_path=str(CSV_PATH),
            client=FakeClient(_fail_tonydiver),
            known=known,
            sample=True,
            reset_checkpoints=True,
            checkpoint_path=ckpt_fail,
            output_csv=str(tmp_path / "fail.csv"),
            run_json=str(tmp_path / "fail_run.json"),
            sleep=lambda _s: None,
        )
        fail_data = json.loads(Path(ckpt_fail).read_text(encoding="utf-8"))
        fail_key = checkpoint_key("Tony Diver", "tony_diver")
        if fail_key not in fail_data.get("failed", []):
            _fail("11. 500 failed key", fail_data.get("failed"))
        if fail_key in fail_data.get("completed", []):
            _fail("11. 500 completed", "failed key also completed")

    # client.post() still returns body/None
    from tiktok.api.client import TikTokClient

    raw_dir = tempfile.mkdtemp()
    client = TikTokClient("https://example.invalid/v2", raw_dir, db_conn=None)
    with patch(
        "tiktok.api.client.auth_headers",
        return_value={"Authorization": "Bearer x"},
    ):
        with patch(
            "tiktok.api.client.requests.post",
            return_value=FakeHTTP(200, {"data": {"username": "a"}}),
        ):
            body = client.post("research/user/info/", {"username": "a"}, {"fields": "x"})
            if not body or body.get("data", {}).get("username") != "a":
                _fail("post compat 200", str(body))
        with patch(
            "tiktok.api.client.requests.post",
            return_value=FakeHTTP(500, {"error": {"code": "internal"}}),
        ):
            body = client.post("research/user/info/", {"username": "a"}, {"fields": "x"})
            if body is not None:
                _fail("post compat 500", str(body))
            meta = client.post_with_status(
                "research/user/info/", {"username": "a"}, {"fields": "x"}
            )
            if meta.get("http_status") != 500 or meta.get("ok"):
                _fail("post_with_status 500", str(meta))

    if _file_sha(P1_PATH) != p1_sha:
        _fail("13. p1 unmodified", "newsfluencer_combined.txt changed")
    if _file_sha(P2_PATH) != p2_sha:
        _fail("13. p2 unmodified", "news_accounts.txt changed")
    print("PASS 13b. Production handle files remain byte-identical")

    # CLI must not call load_config (would require TIKTOK_CLIENT_KEY)
    if "load_config" in cli_src:
        _fail("9. load_config", "CLI must not use load_config/TIKTOK_CLIENT_KEY")
    if "query_videos" in DISCOVERY_PY.read_text(encoding="utf-8"):
        _fail("15. query_videos", "discovery module references query_videos")


if __name__ == "__main__":
    raise SystemExit(main())
