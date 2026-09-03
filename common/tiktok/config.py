"""Load common/config.yaml and .env into a typed Config object."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from dotenv import load_dotenv

from tiktok.paths import repo_root


@dataclass
class Config:
    base_url: str
    start_date: str
    end_date: str
    handle_groups: Dict[str, List[str]]
    defaults: Dict[str, str]
    classification: dict
    transcription: dict
    paths: dict

    tiktok_client_key: str
    tiktok_client_secret: str
    openai_api_key: str

    keywords: dict = field(default_factory=dict)
    collection_pipelines: dict = field(default_factory=dict)
    research_timezone: str = "America/Chicago"
    enrichment: dict = field(default_factory=dict)
    box: dict = field(default_factory=dict)
    config_dir: str = "."

    def get_handles(self, group: str) -> List[str]:
        if group not in self.handle_groups:
            available = ", ".join(self.handle_groups.keys())
            raise ValueError(f"Unknown handle group '{group}'. Available: {available}")
        return self.handle_groups[group]

    def default_group(self, script_name: str) -> str:
        return self.defaults.get(script_name, "batch_test")

    def get_keywords(self, source: Optional[str] = None) -> List[str]:
        sources = (self.keywords or {}).get("sources") or {}
        name = source or (self.keywords or {}).get("default_source")
        if not name:
            raise ValueError("No keyword source configured (keywords.default_source)")
        if name not in sources:
            available = ", ".join(sources.keys()) or "(none)"
            raise ValueError(f"Unknown keyword source '{name}'. Available: {available}")

        rel = sources[name].get("path")
        if not rel:
            raise ValueError(f"Keyword source '{name}' has no path")
        path = Path(rel)
        if not path.is_absolute():
            path = Path(self.config_dir) / path
        if not path.exists():
            raise FileNotFoundError(f"Keyword file not found: {path}")

        terms: List[str] = []
        for line in path.read_text().splitlines():
            term = line.strip()
            if term and not term.startswith("#"):
                terms.append(term)
        return terms


def _resolve_config_path(config_path: str) -> Path:
    raw = Path(config_path)
    if raw.is_file():
        return raw.resolve()
    root = repo_root()
    if config_path in ("", "config.yaml", "common/config.yaml"):
        candidate = root / "common" / "config.yaml"
        if candidate.is_file():
            return candidate
    if not raw.is_absolute():
        candidate = root / config_path
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"Config file not found: {config_path}")


def load_config(config_path: str = "common/config.yaml") -> Config:
    """Load configuration from YAML file and environment variables."""
    load_dotenv()

    config_file = _resolve_config_path(config_path)
    with config_file.open() as f:
        raw = yaml.safe_load(f) or {}

    end_date = (raw.get("date_range") or {}).get("end") or "today"
    if end_date == "today":
        end_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    start_date = (raw.get("date_range") or {}).get("start") or "20260101"
    tiktok = raw.get("tiktok") or {}

    return Config(
        base_url=tiktok.get("base_url") or "https://open.tiktokapis.com/v2",
        start_date=start_date,
        end_date=end_date,
        handle_groups=raw.get("handle_groups") or {},
        defaults=raw.get("defaults") or {},
        classification=raw.get("classification") or {},
        transcription=raw.get("transcription") or {},
        paths=raw.get("paths") or {},
        tiktok_client_key=(os.environ.get("TIKTOK_CLIENT_KEY") or "").strip(),
        tiktok_client_secret=(os.environ.get("TIKTOK_CLIENT_SECRET") or "").strip(),
        openai_api_key=(os.environ.get("OPENAI_API_KEY") or "").strip(),
        keywords=raw.get("keywords") or {},
        collection_pipelines=raw.get("collection_pipelines") or {},
        research_timezone=(
            (raw.get("research") or {}).get("timezone")
            or "America/Chicago"
        ),
        enrichment=raw.get("enrichment") or {},
        box=raw.get("box") or {},
        config_dir=str(repo_root()),
    )
