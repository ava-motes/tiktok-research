"""Collection pipeline registry (content_creators / news_accounts / keyword_search).

Pipelines share the same TikTok Research API credentials for now (server .env).
Optional per-pipeline env key names can be set in config.yaml so separate
CONTENT_CREATOR / NEWS / KEYWORD credentials can be wired later without
changing callers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from tiktok.config import Config


PIPELINE_CONTENT_CREATORS = "content_creators"
PIPELINE_NEWS_ACCOUNTS = "news_accounts"
PIPELINE_KEYWORD_SEARCH = "keyword_search"


def normalize_handle(raw: str) -> str:
    """Normalize creator username for exclusion comparisons."""
    return (raw or "").strip().lstrip("@").strip().lower()


def is_collectable_handle(raw: str) -> bool:
    """False for notes accidentally stored as handles (do not guess replacements)."""
    n = normalize_handle(raw)
    if not n:
        return False
    if " " in n or "(" in n or ")" in n:
        return False
    return True


@dataclass(frozen=True)
class PipelineSpec:
    id: str
    export_dir: str
    description: str = ""
    handle_group: str = ""
    sample_handle_group: str = ""
    client_key_env: Optional[str] = None
    client_secret_env: Optional[str] = None
    api_source: str = ""
    # Keyword pipeline only (unused until Pipeline 3 is implemented)
    keyword_source: str = ""
    sample_keyword_limit: int = 5
    max_videos_per_keyword: int = 50
    exclude_handle_groups: tuple = field(default_factory=tuple)

    def resolve_handles(self, cfg: Config, *, sample: bool) -> List[str]:
        group = self.sample_handle_group if sample else self.handle_group
        if not group:
            raise ValueError(f"Pipeline '{self.id}' has no handle_group configured")
        return list(cfg.get_handles(group))

    def resolve_handle_group_name(self, *, sample: bool) -> str:
        name = self.sample_handle_group if sample else self.handle_group
        if not name:
            raise ValueError(f"Pipeline '{self.id}' has no handle_group configured")
        return name

    def resolved_api_source(self) -> str:
        if self.api_source:
            return self.api_source
        mapping = {
            PIPELINE_CONTENT_CREATORS: "CONTENT_CREATOR_API",
            PIPELINE_NEWS_ACCOUNTS: "NEWS_API",
            PIPELINE_KEYWORD_SEARCH: "KEYWORD_SEARCH_API",
        }
        return mapping.get(self.id, self.id.upper())

    def resolve_keywords(self, cfg: Config, *, sample: bool) -> List[str]:
        source = self.keyword_source or None
        terms = list(cfg.get_keywords(source))
        if sample and self.sample_keyword_limit > 0:
            return terms[: self.sample_keyword_limit]
        return terms

    def exclusion_handles(self, cfg: Config) -> set:
        """Normalized usernames to exclude from keyword search results."""
        out: set = set()
        for group in self.exclude_handle_groups:
            if group not in cfg.handle_groups:
                continue
            for h in cfg.handle_groups[group]:
                n = normalize_handle(h)
                if n:
                    out.add(n)
        return out

    def resolve_credentials(self, cfg: Config) -> tuple[str, str]:
        """Return (client_key, client_secret). Falls back to shared TikTok env."""
        key = None
        secret = None
        if self.client_key_env:
            key = os.environ.get(self.client_key_env)
        if self.client_secret_env:
            secret = os.environ.get(self.client_secret_env)
        if not key:
            key = cfg.tiktok_client_key
        if not secret:
            secret = cfg.tiktok_client_secret
        if not key or not secret:
            raise RuntimeError(
                f"Missing TikTok credentials for pipeline '{self.id}'. "
                "Set TIKTOK_CLIENT_KEY/SECRET (or pipeline-specific env vars)."
            )
        return key, secret


def _raw_pipelines(cfg: Config) -> Dict[str, Any]:
    return getattr(cfg, "collection_pipelines", None) or {}


def get_pipeline(cfg: Config, pipeline_id: str) -> PipelineSpec:
    raw = _raw_pipelines(cfg).get(pipeline_id)
    if not raw:
        available = ", ".join(sorted(_raw_pipelines(cfg))) or "(none)"
        raise ValueError(
            f"Unknown collection pipeline '{pipeline_id}'. Available: {available}"
        )
    excludes = raw.get("exclude_handle_groups") or []
    return PipelineSpec(
        id=raw.get("id") or pipeline_id,
        export_dir=raw.get("export_dir")
        or os.path.join(cfg.paths.get("exports", "data/exports"), pipeline_id),
        description=raw.get("description") or "",
        handle_group=raw.get("handle_group") or "",
        sample_handle_group=raw.get("sample_handle_group")
        or raw.get("handle_group")
        or "",
        client_key_env=raw.get("client_key_env"),
        client_secret_env=raw.get("client_secret_env"),
        api_source=raw.get("api_source") or "",
        keyword_source=raw.get("keyword_source") or "",
        sample_keyword_limit=int(raw.get("sample_keyword_limit") or 5),
        max_videos_per_keyword=int(raw.get("max_videos_per_keyword") or 50),
        exclude_handle_groups=tuple(excludes),
    )
