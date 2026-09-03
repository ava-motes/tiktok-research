"""Put ``common/`` on sys.path and chdir to the repository root.

Entry scripts that live outside ``common/`` should load this file by path:

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
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def repo_root() -> Path:
    here = Path(__file__).resolve().parent.parent
    if not (here / "common" / "tiktok").is_dir() or not (here / "p1_content_creators").is_dir():
        raise RuntimeError(f"Not the tiktok_research root: {here}")
    return here


def setup() -> Path:
    root = repo_root()
    common = str(root / "common")
    if common not in sys.path:
        sys.path.insert(0, common)
    os.chdir(root)
    return root


DEFAULT_CONFIG = "common/config.yaml"
