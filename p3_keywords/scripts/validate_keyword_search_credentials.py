"""Static credential-isolation checks for Pipeline 3.

No TikTok API, media, enrichment, BigQuery, or collection.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional
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

P1_KEY, P1_SECRET = "p1-placeholder-key", "p1-placeholder-secret"
P1_OVERRIDE_KEY, P1_OVERRIDE_SECRET = "p1-override-key", "p1-override-secret"
P2_KEY, P2_SECRET = "p2-placeholder-key", "p2-placeholder-secret"
P3_KEY, P3_SECRET = "p3-placeholder-key", "p3-placeholder-secret"

CRED_ENVS = (
    "KEYWORD_SEARCH_API_CLIENT_KEY",
    "KEYWORD_SEARCH_API_CLIENT_SECRET",
    "TIKTOK_CLIENT_KEY",
    "TIKTOK_CLIENT_SECRET",
    "NEWS_API_CLIENT_KEY",
    "NEWS_API_CLIENT_SECRET",
    "CONTENT_CREATOR_TIKTOK_CLIENT_KEY",
    "CONTENT_CREATOR_TIKTOK_CLIENT_SECRET",
    "OPENAI_API_KEY",
    "TIKTOK_ALLOW_LOCAL_COLLECTION",
)


def _fail(name: str, msg: str) -> None:
    raise AssertionError(f"{name}: {msg}")


def _snapshot_env() -> Dict[str, Optional[str]]:
    return {k: os.environ.get(k) for k in CRED_ENVS}


def _restore_env(saved: Dict[str, Optional[str]]) -> None:
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def _clear_cred_env() -> None:
    for k in CRED_ENVS:
        os.environ.pop(k, None)


def main() -> int:
    os.environ.pop("TIKTOK_ALLOW_LOCAL_COLLECTION", None)
    network: List[Any] = []

    def _blocked(*args, **kwargs):
        network.append((args, kwargs))
        raise AssertionError("network call during credential static tests")

    with patch("requests.post", side_effect=_blocked), patch(
        "tiktok.auth.init", side_effect=_blocked
    ), patch("tiktok.auth.get_access_token", side_effect=_blocked), patch(
        "urllib.request.urlopen", side_effect=_blocked
    ):
        _run_checks()

    if network:
        _fail("12. no network", f"blocked call invoked {len(network)} time(s)")
    print("PASS 12. no network calls during tests")
    print("All Pipeline 3 credential isolation checks passed.")
    return 0


def _run_checks() -> None:
    saved = _snapshot_env()
    try:
        os.environ.setdefault("TIKTOK_CLIENT_KEY", P1_KEY)
        os.environ.setdefault("TIKTOK_CLIENT_SECRET", P1_SECRET)
        os.environ.setdefault("OPENAI_API_KEY", "static-test-placeholder")

        from tiktok.config import load_config
        from tiktok.pipelines import (
            KEYWORD_SEARCH_CLIENT_KEY_ENV,
            KEYWORD_SEARCH_CLIENT_SECRET_ENV,
            MISSING_KEYWORD_SEARCH_CREDENTIALS,
            P3_FORBIDDEN_CREDENTIAL_ENVS,
            PIPELINE_CONTENT_CREATORS,
            PIPELINE_KEYWORD_SEARCH,
            PIPELINE_NEWS_ACCOUNTS,
            get_pipeline,
            require_keyword_search_credentials,
        )

        cfg = load_config("common/config.yaml")
        p1 = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
        p2 = get_pipeline(cfg, PIPELINE_NEWS_ACCOUNTS)
        p3 = get_pipeline(cfg, PIPELINE_KEYWORD_SEARCH)

        # Mapping from config
        if p3.id != "keyword":
            _fail("9. pipeline_id", f"got {p3.id}")
        if p3.resolved_api_source() != "KEYWORD_SEARCH_API":
            _fail("8. api_source", f"got {p3.resolved_api_source()}")
        if p3.client_key_env != KEYWORD_SEARCH_CLIENT_KEY_ENV:
            _fail("1. P3 key env", f"got {p3.client_key_env}")
        if p3.client_secret_env != KEYWORD_SEARCH_CLIENT_SECRET_ENV:
            _fail("2. P3 secret env", f"got {p3.client_secret_env}")
        if not p3.require_dedicated_credentials:
            _fail("1. P3 key env", "require_dedicated_credentials is false")
        print("PASS 8. api_source=KEYWORD_SEARCH_API")
        print("PASS 9. pipeline_id=keyword")

        # All three pipelines set to distinct placeholders
        os.environ["TIKTOK_CLIENT_KEY"] = P1_KEY
        os.environ["TIKTOK_CLIENT_SECRET"] = P1_SECRET
        os.environ.pop("CONTENT_CREATOR_TIKTOK_CLIENT_KEY", None)
        os.environ.pop("CONTENT_CREATOR_TIKTOK_CLIENT_SECRET", None)
        os.environ["NEWS_API_CLIENT_KEY"] = P2_KEY
        os.environ["NEWS_API_CLIENT_SECRET"] = P2_SECRET
        os.environ["KEYWORD_SEARCH_API_CLIENT_KEY"] = P3_KEY
        os.environ["KEYWORD_SEARCH_API_CLIENT_SECRET"] = P3_SECRET
        # Reload cfg so P1 fallback sees the placeholder TIKTOK_CLIENT_* values
        cfg = load_config("common/config.yaml")
        p1 = get_pipeline(cfg, PIPELINE_CONTENT_CREATORS)
        p2 = get_pipeline(cfg, PIPELINE_NEWS_ACCOUNTS)
        p3 = get_pipeline(cfg, PIPELINE_KEYWORD_SEARCH)

        p3_pair = p3.resolve_credentials(cfg)
        if p3_pair != (P3_KEY, P3_SECRET):
            _fail("1. P3 uses KEYWORD_SEARCH_API_CLIENT_KEY", "resolved pair is not the P3 placeholders")
        if p3_pair[0] == P1_KEY or p3_pair[1] == P1_SECRET:
            _fail("3. no P1 fallback", "P3 resolved a Pipeline 1 placeholder")
        if p3_pair[0] == P2_KEY or p3_pair[1] == P2_SECRET:
            _fail("4. no P2 fallback", "P3 resolved a Pipeline 2 placeholder")
        direct = require_keyword_search_credentials(p3)
        if direct != (P3_KEY, P3_SECRET):
            _fail("1. P3 key env", "require_keyword_search_credentials mismatch")

        forbidden_reads: List[str] = []
        real_get = os.environ.get

        def _spy_get(key, default=None):
            if key in P3_FORBIDDEN_CREDENTIAL_ENVS:
                forbidden_reads.append(str(key))
            return real_get(key, default)

        with patch.object(os.environ, "get", side_effect=_spy_get):
            p3.resolve_credentials(cfg)
            require_keyword_search_credentials(p3)
        if forbidden_reads:
            _fail(
                "3. no P1 fallback",
                f"P3 credential resolve read forbidden env vars: {forbidden_reads}",
            )
        print("PASS 1. P3 uses KEYWORD_SEARCH_API_CLIENT_KEY")
        print("PASS 2. P3 uses KEYWORD_SEARCH_API_CLIENT_SECRET")

        p1_pair = p1.resolve_credentials(cfg)
        if p1_pair != (P1_KEY, P1_SECRET):
            _fail("6. P1 unchanged", "P1 fallback did not use TIKTOK_CLIENT_*")
        os.environ["CONTENT_CREATOR_TIKTOK_CLIENT_KEY"] = P1_OVERRIDE_KEY
        os.environ["CONTENT_CREATOR_TIKTOK_CLIENT_SECRET"] = P1_OVERRIDE_SECRET
        p1_override = p1.resolve_credentials(cfg)
        if p1_override != (P1_OVERRIDE_KEY, P1_OVERRIDE_SECRET):
            _fail("6. P1 unchanged", "P1 override env vars were ignored")
        os.environ.pop("CONTENT_CREATOR_TIKTOK_CLIENT_KEY", None)
        os.environ.pop("CONTENT_CREATOR_TIKTOK_CLIENT_SECRET", None)
        if p1.require_dedicated_credentials:
            _fail("6. P1 unchanged", "P1 unexpectedly requires dedicated credentials")
        if p1.resolved_api_source() != "CONTENT_CREATOR_API":
            _fail("6. P1 unchanged", f"api_source={p1.resolved_api_source()}")
        print("PASS 6. P1 credential behavior is unchanged")

        p2_pair = p2.resolve_credentials(cfg)
        if p2_pair != (P2_KEY, P2_SECRET):
            _fail("7. P2 unchanged", "P2 did not use NEWS_API_* placeholders")
        if not p2.require_dedicated_credentials:
            _fail("7. P2 unchanged", "P2 require_dedicated_credentials is false")
        if p2.client_key_env != "NEWS_API_CLIENT_KEY" or p2.client_secret_env != "NEWS_API_CLIENT_SECRET":
            _fail("7. P2 unchanged", "P2 env names changed")
        if p2.resolved_api_source() != "NEWS_API":
            _fail("7. P2 unchanged", f"api_source={p2.resolved_api_source()}")
        print("PASS 7. P2 credential behavior is unchanged")

        # P3 missing, P1+P2 present → fail, no auth
        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_KEY", None)
        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_SECRET", None)
        os.environ["TIKTOK_CLIENT_KEY"] = P1_KEY
        os.environ["TIKTOK_CLIENT_SECRET"] = P1_SECRET
        os.environ["NEWS_API_CLIENT_KEY"] = P2_KEY
        os.environ["NEWS_API_CLIENT_SECRET"] = P2_SECRET
        try:
            p3.resolve_credentials(cfg)
            _fail("3. no P1 fallback", "P3 succeeded with only P1/P2 credentials")
        except RuntimeError as e:
            if str(e) != MISSING_KEYWORD_SEARCH_CREDENTIALS:
                _fail("5. fail before API", f"unexpected error text: {e}")
        try:
            require_keyword_search_credentials()
            _fail("5. fail before API", "require_keyword_search_credentials succeeded")
        except RuntimeError as e:
            if "KEYWORD_SEARCH_API_CLIENT_KEY" not in str(e):
                _fail("5. fail before API", f"unexpected error: {e}")
            if "Pipeline 1" not in str(e) or "Pipeline 2" not in str(e):
                _fail("5. fail before API", "error did not mention P1/P2 non-fallback")
        os.environ["CONTENT_CREATOR_TIKTOK_CLIENT_KEY"] = P1_OVERRIDE_KEY
        os.environ["CONTENT_CREATOR_TIKTOK_CLIENT_SECRET"] = P1_OVERRIDE_SECRET
        try:
            p3.resolve_credentials(cfg)
            _fail("3. no P1 fallback", "dummy CONTENT_CREATOR_* credentials made P3 succeed")
        except RuntimeError as e:
            if str(e) != MISSING_KEYWORD_SEARCH_CREDENTIALS:
                _fail("3. no P1 fallback", f"unexpected error with dummy P1 override: {e}")
        os.environ.pop("CONTENT_CREATOR_TIKTOK_CLIENT_KEY", None)
        os.environ.pop("CONTENT_CREATOR_TIKTOK_CLIENT_SECRET", None)
        print("PASS 3. P3 never falls back to P1 credentials")
        print("PASS 4. P3 never falls back to P2 credentials")

        # P2 missing, P1+P3 present → P2 still fails (unchanged); P3 still works
        os.environ["KEYWORD_SEARCH_API_CLIENT_KEY"] = P3_KEY
        os.environ["KEYWORD_SEARCH_API_CLIENT_SECRET"] = P3_SECRET
        os.environ.pop("NEWS_API_CLIENT_KEY", None)
        os.environ.pop("NEWS_API_CLIENT_SECRET", None)
        try:
            p2.resolve_credentials(cfg)
            _fail("7. P2 unchanged", "P2 succeeded without NEWS_API_*")
        except RuntimeError as e:
            msg = str(e)
            if "NEWS_API_CLIENT_KEY" not in msg:
                _fail("7. P2 unchanged", f"P2 error missing NEWS_API name: {msg}")
            if "KEYWORD_SEARCH" in msg:
                _fail("7. P2 unchanged", "P2 error mentioned Pipeline 3")
        if p3.resolve_credentials(cfg) != (P3_KEY, P3_SECRET):
            _fail("1. P3 key env", "P3 failed while P2 was unset")

        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_KEY", None)
        os.environ.pop("KEYWORD_SEARCH_API_CLIENT_SECRET", None)
        os.environ["TIKTOK_ALLOW_LOCAL_COLLECTION"] = "1"
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "run_keyword_under_test",
            os.path.join(ROOT, "p3_keywords/scripts/run_keyword.py"),
        )
        runner = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(runner)
        with patch.object(
            sys, "argv", ["run_keyword.py", "--date", "2026-08-13", "--sample"]
        ):
            rc = runner.main()
        os.environ.pop("TIKTOK_ALLOW_LOCAL_COLLECTION", None)
        if rc != 2:
            _fail("5. fail before API", f"runner exit {rc}, expected 2")
        print("PASS 5. missing P3 credentials fail before authentication/API calls")

        from tiktok.collection.server_guard import require_collection_server

        os.environ.pop("TIKTOK_ALLOW_LOCAL_COLLECTION", None)
        with patch(
            "tiktok.collection.server_guard.socket.gethostname",
            return_value="comm-a92978",
        ):
            try:
                require_collection_server()
                _fail("host guard", "comm-a92978 was allowed to collect")
            except SystemExit as e:
                msg = str(e)
                if "Refusing TikTok collection" not in msg:
                    _fail("host guard", f"unexpected exit: {msg}")
                if "comm-a92978" not in msg.lower():
                    _fail("host guard", f"exit did not name comm-a92978: {msg}")
        print("PASS host guard: comm-a92978 cannot collect")

        _check_no_secrets_in_tracked_files()
        print("PASS 10. no secrets are present in tracked files")

        _check_p1_p2_runners_untouched()
        print("PASS 11. existing P1/P2 runners remain untouched")
    finally:
        _restore_env(saved)


def _check_no_secrets_in_tracked_files() -> None:
    try:
        listed = subprocess.check_output(
            ["git", "ls-files"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).splitlines()
    except (subprocess.CalledProcessError, FileNotFoundError):
        listed = []
    if ".env" in listed:
        _fail("10. no secrets", ".env is tracked")
    example = os.path.join(ROOT, ".env.example")
    text = open(example, encoding="utf-8").read()
    for name in (
        "KEYWORD_SEARCH_API_CLIENT_KEY",
        "KEYWORD_SEARCH_API_CLIENT_SECRET",
    ):
        if not re.search(rf"^{re.escape(name)}=$", text, re.M):
            _fail("10. no secrets", f".env.example must contain empty {name}=")
    assign = re.compile(
        r"^(KEYWORD_SEARCH_API_CLIENT_KEY|KEYWORD_SEARCH_API_CLIENT_SECRET)"
        r"[ \t]*=[ \t]*(.*)$",
        re.M,
    )
    for rel in listed:
        if not rel or rel.startswith("data/"):
            continue
        path = os.path.join(ROOT, rel)
        if not os.path.isfile(path):
            continue
        try:
            body = open(path, encoding="utf-8").read()
        except (OSError, UnicodeDecodeError):
            continue
        for match in assign.finditer(body):
            value = match.group(2).strip().strip("'\"")
            if value:
                _fail(
                    "10. no secrets",
                    f"non-empty {match.group(1)} assignment in tracked {rel}",
                )


def _check_p1_p2_runners_untouched() -> None:
    runners = [
        "p1_content_creators/scripts/run_content_creators.py",
        "p2_news/scripts/run_news.py",
        "p2_news/scripts/collect_news.py",
        "p1_content_creators/scripts/collect_content_creators.py",
    ]
    for rel in runners:
        path = os.path.join(ROOT, rel)
        if not os.path.isfile(path):
            continue
        body = open(path, encoding="utf-8").read()
        if "KEYWORD_SEARCH_API" in body:
            _fail("11. P1/P2 runners", f"{rel} references Pipeline 3 credentials")
    p1 = open(os.path.join(ROOT, "p1_content_creators/scripts/run_content_creators.py"), encoding="utf-8").read()
    if "PIPELINE_CONTENT_CREATORS" not in p1:
        _fail("11. P1/P2 runners", "run_content_creators.py lost Pipeline 1 identity")
    p2 = open(os.path.join(ROOT, "p2_news/scripts/run_news.py"), encoding="utf-8").read()
    if "NEWS_API_CLIENT_KEY" not in p2 or "PIPELINE_NEWS" not in p2:
        _fail("11. P1/P2 runners", "run_news.py lost Pipeline 2 credential guard")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as e:
        print(f"FAIL {e}", file=sys.stderr)
        raise SystemExit(1)
