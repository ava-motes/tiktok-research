"""Load config.yaml and .env into a typed Config object."""

import os
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from dotenv import load_dotenv


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

    # Secrets from .env
    tiktok_client_key: str
    tiktok_client_secret: str
    openai_api_key: str

    # Optional keyword sources (keyword collection only; unused by handle pulls)
    keywords: dict = field(default_factory=dict)
    # Daily collection pipelines (content_creators / news_accounts / keyword_search)
    collection_pipelines: dict = field(default_factory=dict)
    research_timezone: str = "America/Chicago"
    enrichment: dict = field(default_factory=dict)
    config_dir: str = "."

    def get_handles(self, group: str) -> List[str]:
        """Return the handle list for a named group."""
        if group not in self.handle_groups:
            available = ", ".join(self.handle_groups.keys())
            raise ValueError(f"Unknown handle group '{group}'. Available: {available}")
        return self.handle_groups[group]

    def default_group(self, script_name: str) -> str:
        """Return the default handle group for a given script."""
        return self.defaults.get(script_name, "sample")

    def get_keywords(self, source: Optional[str] = None) -> List[str]:
        """Return keyword terms for a named source (keyword collection only).

        Does not affect handle-based collection workflows.
        """
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


def load_config(config_path: str = "config.yaml") -> Config:
    """Load configuration from YAML file and environment variables."""
    load_dotenv()

    config_file = Path(config_path)
    with config_file.open() as f:
        raw = yaml.safe_load(f)

    end_date = raw["date_range"]["end"]
    if end_date == "today":
        end_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    return Config(
        base_url=raw["tiktok"]["base_url"],
        start_date=raw["date_range"]["start"],
        end_date=end_date,
        handle_groups=raw["handle_groups"],
        defaults=raw["defaults"],
        classification=raw["classification"],
        transcription=raw["transcription"],
        paths=raw["paths"],
        tiktok_client_key=os.environ["TIKTOK_CLIENT_KEY"],
        tiktok_client_secret=os.environ["TIKTOK_CLIENT_SECRET"],
        openai_api_key=os.environ["OPENAI_API_KEY"],
        keywords=raw.get("keywords") or {},
        collection_pipelines=raw.get("collection_pipelines") or {},
        research_timezone=(
            (raw.get("research") or {}).get("timezone")
            or "America/Chicago"
        ),
        enrichment=raw.get("enrichment") or {},
        config_dir=str(config_file.resolve().parent),
    )
