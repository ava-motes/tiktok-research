"""Load config.yaml and .env into a typed Config object."""

import os
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Dict, List

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

    def get_handles(self, group: str) -> List[str]:
        """Return the handle list for a named group."""
        if group not in self.handle_groups:
            available = ", ".join(self.handle_groups.keys())
            raise ValueError(f"Unknown handle group '{group}'. Available: {available}")
        return self.handle_groups[group]

    def default_group(self, script_name: str) -> str:
        """Return the default handle group for a given script."""
        return self.defaults.get(script_name, "sample")


def load_config(config_path: str = "config.yaml") -> Config:
    """Load configuration from YAML file and environment variables."""
    load_dotenv()

    with open(config_path) as f:
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
    )
