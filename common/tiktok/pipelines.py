"""Collection pipeline registry (content_creators / news / keyword).

Pipeline-specific TikTok Research API credentials are named in config.yaml.
Pipeline 2 (news) requires dedicated NEWS_API_* env vars and never
falls back to Pipeline 1 or Pipeline 3 keys.
Pipeline 3 (keyword) requires dedicated KEYWORD_SEARCH_API_* env vars
and never falls back to Pipeline 1 or Pipeline 2 keys.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from tiktok.config import Config


PIPELINE_CONTENT_CREATORS = "content_creators"
PIPELINE_NEWS = "news"
PIPELINE_KEYWORD = "keyword"
# Legacy aliases kept so older flags/docs still resolve.
PIPELINE_NEWS_ACCOUNTS = PIPELINE_NEWS
PIPELINE_KEYWORD_SEARCH = PIPELINE_KEYWORD

PIPELINE_ID_ALIASES = {
    "news_accounts": PIPELINE_NEWS,
    "keyword_search": PIPELINE_KEYWORD,
}

NEWS_CLIENT_KEY_ENV = "NEWS_API_CLIENT_KEY"
NEWS_CLIENT_SECRET_ENV = "NEWS_API_CLIENT_SECRET"
KEYWORD_SEARCH_CLIENT_KEY_ENV = "KEYWORD_SEARCH_API_CLIENT_KEY"
KEYWORD_SEARCH_CLIENT_SECRET_ENV = "KEYWORD_SEARCH_API_CLIENT_SECRET"

P2_FORBIDDEN_CREDENTIAL_ENVS = frozenset(
    {
        "TIKTOK_CLIENT_KEY",
        "TIKTOK_CLIENT_SECRET",
        "CONTENT_CREATOR_TIKTOK_CLIENT_KEY",
        "CONTENT_CREATOR_TIKTOK_CLIENT_SECRET",
        "KEYWORD_SEARCH_API_CLIENT_KEY",
        "KEYWORD_SEARCH_API_CLIENT_SECRET",
    }
)
P3_FORBIDDEN_CREDENTIAL_ENVS = frozenset(
    {
        "TIKTOK_CLIENT_KEY",
        "TIKTOK_CLIENT_SECRET",
        "CONTENT_CREATOR_TIKTOK_CLIENT_KEY",
        "CONTENT_CREATOR_TIKTOK_CLIENT_SECRET",
        "NEWS_API_CLIENT_KEY",
        "NEWS_API_CLIENT_SECRET",
    }
)

MISSING_NEWS_CREDENTIALS = (
    "Missing required Pipeline 2 credentials:\n"
    f"{NEWS_CLIENT_KEY_ENV} / {NEWS_CLIENT_SECRET_ENV}\n"
    "\n"
    "Pipeline 2 will not fall back to Pipeline 1 or Pipeline 3 credentials."
)
MISSING_KEYWORD_SEARCH_CREDENTIALS = (
    "Missing required Pipeline 3 credentials:\n"
    f"{KEYWORD_SEARCH_CLIENT_KEY_ENV} / {KEYWORD_SEARCH_CLIENT_SECRET_ENV}\n"
    "\n"
    "Pipeline 3 will not fall back to Pipeline 1 or Pipeline 2 credentials."
)


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


def load_handle_file(path: str) -> List[str]:
    """Load handles from a text file: one per line, # comments, order-preserving dedupe."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Handle file not found: {path}")
    out: List[str] = []
    seen = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            n = normalize_handle(raw)
            if not n or n in seen:
                continue
            seen.add(n)
            out.append(n)
    return out


def load_keyword_file(path: str) -> List[str]:
    """Load keyword terms: lowercase, skip blanks/comments, order-preserving dedupe.

    ``.csv`` files use the ``term`` column (or the first column). Counts and
    other columns are ignored so ``trump,3834`` is not treated as a keyword.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Keyword file not found: {path}")
    terms: List[str] = []
    seen = set()

    def _add(raw: str) -> None:
        n = (raw or "").strip().lower()
        if not n or n.startswith("#"):
            return
        if n in seen:
            return
        seen.add(n)
        terms.append(n)

    if path.lower().endswith(".csv"):
        import csv

        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            names = list(reader.fieldnames or [])
            if not names:
                raise ValueError(f"Keyword CSV has no header: {path}")
            term_key = next(
                (name for name in names if (name or "").strip().lower() == "term"),
                names[0],
            )
            for row in reader:
                _add(row.get(term_key) or "")
        return terms

    with open(path, encoding="utf-8") as f:
        for line in f:
            _add(line)
    return terms


def _resolve_repo_path(cfg: Config, rel: str) -> str:
    if os.path.isabs(rel):
        return rel
    return os.path.join(cfg.config_dir, rel)


def require_news_credentials(pipeline: Optional["PipelineSpec"] = None) -> tuple[str, str]:
    """Return Pipeline 2 credentials. Never reads P1/P3 env vars. No OAuth/API.

    Call this before auth.init() or any TikTok request. Missing values raise
    MISSING_NEWS_CREDENTIALS.
    """
    if pipeline is not None:
        if pipeline.id != PIPELINE_NEWS:
            raise RuntimeError(
                f"require_news_credentials is for '{PIPELINE_NEWS}', "
                f"got '{pipeline.id}'"
            )
        if not pipeline.require_dedicated_credentials:
            raise RuntimeError(
                "Pipeline 2 must set require_dedicated_credentials: true. "
                "Refusing to use any other pipeline's credentials."
            )
        if (pipeline.client_key_env or "").strip() != NEWS_CLIENT_KEY_ENV:
            raise RuntimeError(
                f"Pipeline 2 client_key_env must be {NEWS_CLIENT_KEY_ENV}."
            )
        if (pipeline.client_secret_env or "").strip() != NEWS_CLIENT_SECRET_ENV:
            raise RuntimeError(
                f"Pipeline 2 client_secret_env must be {NEWS_CLIENT_SECRET_ENV}."
            )
    key = (os.environ.get(NEWS_CLIENT_KEY_ENV) or "").strip()
    secret = (os.environ.get(NEWS_CLIENT_SECRET_ENV) or "").strip()
    if not key or not secret:
        raise RuntimeError(MISSING_NEWS_CREDENTIALS)
    return key, secret


def require_keyword_search_credentials(pipeline: Optional["PipelineSpec"] = None) -> tuple[str, str]:
    """Return Pipeline 3 credentials. Never reads P1/P2 env vars. No OAuth/API.

    Call this before auth.init() or any TikTok request. Missing values raise
    MISSING_KEYWORD_SEARCH_CREDENTIALS.
    """
    if pipeline is not None:
        if pipeline.id != PIPELINE_KEYWORD:
            raise RuntimeError(
                f"require_keyword_search_credentials is for '{PIPELINE_KEYWORD}', "
                f"got '{pipeline.id}'"
            )
        if not pipeline.require_dedicated_credentials:
            raise RuntimeError(
                "Pipeline 3 must set require_dedicated_credentials: true. "
                "Refusing to use any other pipeline's credentials."
            )
        if (pipeline.client_key_env or "").strip() != KEYWORD_SEARCH_CLIENT_KEY_ENV:
            raise RuntimeError(
                "Pipeline 3 client_key_env must be "
                f"{KEYWORD_SEARCH_CLIENT_KEY_ENV}."
            )
        if (pipeline.client_secret_env or "").strip() != KEYWORD_SEARCH_CLIENT_SECRET_ENV:
            raise RuntimeError(
                "Pipeline 3 client_secret_env must be "
                f"{KEYWORD_SEARCH_CLIENT_SECRET_ENV}."
            )
    key = (os.environ.get(KEYWORD_SEARCH_CLIENT_KEY_ENV) or "").strip()
    secret = (os.environ.get(KEYWORD_SEARCH_CLIENT_SECRET_ENV) or "").strip()
    if not key or not secret:
        raise RuntimeError(MISSING_KEYWORD_SEARCH_CREDENTIALS)
    return key, secret


require_keyword_credentials = require_keyword_search_credentials


@dataclass(frozen=True)
class PipelineSpec:
    id: str
    export_dir: str
    description: str = ""
    handle_group: str = ""
    sample_handle_group: str = ""
    handle_file: Optional[str] = None
    client_key_env: Optional[str] = None
    client_secret_env: Optional[str] = None
    api_source: str = ""
    keyword_source: str = ""
    keyword_file: Optional[str] = None
    sample_keyword_limit: int = 0
    sample_keywords: tuple = field(default_factory=tuple)
    max_videos_per_keyword: int = 0
    exclude_handle_groups: tuple = field(default_factory=tuple)
    exclude_handle_files: tuple = field(default_factory=tuple)
    require_dedicated_credentials: bool = False
    bigquery_table: str = ""
    pipeline_root: str = ""
    summary_dir: str = ""
    parquet_dir: str = ""
    log_dir: str = ""
    box_dir: str = ""
    checkpoint_dir: str = ""

    def resolve_handles(self, cfg: Config, *, sample: bool) -> List[str]:
        if sample:
            group = self.sample_handle_group or self.handle_group
            if not group:
                raise ValueError(f"Pipeline '{self.id}' has no sample handle_group configured")
            return list(cfg.get_handles(group))
        if self.handle_file:
            path = self.handle_file
            if not os.path.isabs(path):
                path = os.path.join(cfg.config_dir, path)
            return load_handle_file(path)
        group = self.handle_group
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

    def resolve_path(self, cfg: Config, rel: str) -> str:
        if not rel:
            return ""
        if os.path.isabs(rel):
            return rel
        return os.path.join(cfg.config_dir, rel)

    def resolved_export_dir(self, cfg: Config) -> str:
        override = (cfg.paths or {}).get("exports") or ""
        if os.path.isabs(override):
            return override
        return self.resolve_path(cfg, self.export_dir)

    def resolved_summary_dir(self, cfg: Config) -> str:
        override = (cfg.paths or {}).get("exports") or ""
        if os.path.isabs(override):
            return override
        return self.resolve_path(cfg, self.summary_dir or self.export_dir)

    def resolved_checkpoint_dir(self, cfg: Config) -> str:
        override = (cfg.paths or {}).get("checkpoints") or ""
        if os.path.isabs(override):
            return override
        return self.resolve_path(
            cfg,
            self.checkpoint_dir or override or "data/checkpoints",
        )

    def resolved_box_dir(self, cfg: Config) -> str:
        return self.resolve_path(cfg, self.box_dir or self.export_dir)

    def resolved_log_dir(self, cfg: Config) -> str:
        return self.resolve_path(cfg, self.log_dir or "logs")

    def resolve_keywords(
        self,
        cfg: Config,
        *,
        sample: bool,
        keywords_file: Optional[str] = None,
        limit_keywords: Optional[int] = None,
    ) -> List[str]:
        """Return canonical keyword terms (or the explicit sample list).

        ``--sample`` uses ``sample_keywords`` in listed order. It does **not**
        take the first N terms from the file.
        """
        if keywords_file:
            path = _resolve_repo_path(cfg, keywords_file)
            terms = load_keyword_file(path)
        elif self.keyword_file:
            path = _resolve_repo_path(cfg, self.keyword_file)
            terms = load_keyword_file(path)
        else:
            terms = list(cfg.get_keywords(self.keyword_source or None))
        if sample:
            sample_terms = [str(t).strip() for t in self.sample_keywords if str(t).strip()]
            if not sample_terms:
                raise ValueError(
                    f"Pipeline '{self.id}' --sample requires sample_keywords in config.yaml"
                )
            canonical = set(terms)
            missing = [t for t in sample_terms if t not in canonical]
            if missing:
                raise ValueError(
                    f"Pipeline '{self.id}' sample keywords not in canonical list: {missing}"
                )
            terms = sample_terms
        if limit_keywords is not None:
            if limit_keywords < 0:
                raise ValueError("limit_keywords must be >= 0")
            terms = terms[: int(limit_keywords)]
        return terms

    def effective_max_videos(self) -> Optional[int]:
        """None means unlimited. Positive ints are an explicit cap."""
        n = int(self.max_videos_per_keyword or 0)
        return n if n > 0 else None

    def exclusion_handle_sets(self, cfg: Config) -> Dict[str, List[str]]:
        """Load exclusion usernames from handle **files**, not YAML groups."""
        out: Dict[str, List[str]] = {}
        for label, rel in self.exclude_handle_files:
            path = _resolve_repo_path(cfg, rel)
            out[str(label)] = load_handle_file(path)
        return out

    def exclusion_handles(self, cfg: Config) -> set:
        """Normalized usernames to exclude from keyword search BigQuery rows.

        Pipeline 3 loads ``exclude_handle_files`` (P1 + P2 handle files). YAML
        ``exclude_handle_groups`` is only used if explicitly configured.
        """
        out: set = set()
        for handles in self.exclusion_handle_sets(cfg).values():
            out.update(handles)
        for group in self.exclude_handle_groups:
            if group not in cfg.handle_groups:
                continue
            for h in cfg.handle_groups[group]:
                n = normalize_handle(h)
                if n:
                    out.add(n)
        return out

    def resolve_credentials(self, cfg: Config) -> tuple[str, str]:
        """Return (client_key, client_secret).

        Pipeline 1 may fall back to shared TIKTOK_CLIENT_*. Pipeline 2
        (require_dedicated_credentials) must use NEWS_API_* only.
        Pipeline 3 always uses KEYWORD_SEARCH_API_* only and never reads
        Pipeline 1 or Pipeline 2 environment variables.
        """
        if self.id == PIPELINE_KEYWORD:
            return require_keyword_search_credentials(self)
        if self.id == PIPELINE_NEWS:
            return require_news_credentials(self)
        if self.require_dedicated_credentials:
            raise RuntimeError(
                f"Pipeline '{self.id}' requires dedicated credentials but has "
                "no dedicated resolver. Refusing to fall back to another pipeline."
            )
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
    canonical = PIPELINE_ID_ALIASES.get(pipeline_id, pipeline_id)
    raw = _raw_pipelines(cfg).get(canonical) or _raw_pipelines(cfg).get(pipeline_id)
    if not raw:
        available = ", ".join(sorted(_raw_pipelines(cfg))) or "(none)"
        raise ValueError(
            f"Unknown collection pipeline '{pipeline_id}'. Available: {available}"
        )
    pipeline_id = canonical
    excludes = raw.get("exclude_handle_groups") or []
    raw_files = raw.get("exclude_handle_files") or {}
    if isinstance(raw_files, dict):
        exclude_files = tuple(
            (str(k), str(v)) for k, v in raw_files.items() if v
        )
    else:
        exclude_files = tuple((str(p), str(p)) for p in raw_files if p)
    sample_kw = tuple(
        str(t).strip()
        for t in (raw.get("sample_keywords") or [])
        if str(t).strip()
    )
    raw_max = raw.get("max_videos_per_keyword")
    if raw_max in (None, ""):
        max_videos = 0
    else:
        max_videos = int(raw_max)
    pipeline_root = (raw.get("pipeline_root") or "").strip()
    export_dir = raw.get("export_dir") or (
        os.path.join(pipeline_root, "results", "csv")
        if pipeline_root
        else os.path.join(cfg.paths.get("exports", "data/exports"), pipeline_id)
    )
    summary_dir = raw.get("summary_dir") or (
        os.path.join(pipeline_root, "results", "summaries") if pipeline_root else export_dir
    )
    parquet_dir = raw.get("parquet_dir") or (
        os.path.join(pipeline_root, "results", "parquet") if pipeline_root else export_dir
    )
    log_dir = raw.get("log_dir") or (
        os.path.join(pipeline_root, "logs") if pipeline_root else "logs"
    )
    box_dir = raw.get("box_dir") or (
        os.path.join(pipeline_root, "box") if pipeline_root else export_dir
    )
    checkpoint_dir = raw.get("checkpoint_dir") or (
        os.path.join(pipeline_root, "logs", "checkpoints")
        if pipeline_root
        else cfg.paths.get("checkpoints", "data/checkpoints")
    )
    return PipelineSpec(
        id=raw.get("id") or pipeline_id,
        export_dir=export_dir,
        description=raw.get("description") or "",
        handle_group=raw.get("handle_group") or "",
        sample_handle_group=raw.get("sample_handle_group")
        or raw.get("handle_group")
        or "",
        handle_file=raw.get("handle_file") or None,
        client_key_env=raw.get("client_key_env"),
        client_secret_env=raw.get("client_secret_env"),
        api_source=raw.get("api_source") or "",
        keyword_source=raw.get("keyword_source") or "",
        keyword_file=raw.get("keyword_file") or None,
        sample_keyword_limit=int(raw.get("sample_keyword_limit") or 0),
        sample_keywords=sample_kw,
        max_videos_per_keyword=max_videos,
        exclude_handle_groups=tuple(excludes),
        exclude_handle_files=exclude_files,
        require_dedicated_credentials=bool(
            raw.get("require_dedicated_credentials")
        ),
        bigquery_table=raw.get("bigquery_table") or "",
        pipeline_root=pipeline_root,
        summary_dir=summary_dir,
        parquet_dir=parquet_dir,
        log_dir=log_dir,
        box_dir=box_dir,
        checkpoint_dir=checkpoint_dir,
    )
