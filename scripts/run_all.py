"""Run the full pipeline: pull videos, pull user info, classify, export.

DEPRECATED (pre-v5.0 collect+classify+CSV orchestration). The canonical
orchestrator is `scripts/enrich_pipeline.py` (use `--production`). Kept for the
legacy classify/CSV workflow only. See docs/SCRIPTS.md.

Usage:
    python scripts/run_all.py                # Uses default groups from config
    python scripts/run_all.py --group sample # Override group for all steps
"""

import sys
import os
import argparse
import logging
import subprocess

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.logging_setup import setup_logging

logger = logging.getLogger(__name__)

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))


def run_script(name: str, extra_args: list = None):
    """Run a script as a subprocess, streaming output."""
    script_path = os.path.join(SCRIPTS_DIR, name)
    cmd = [sys.executable, "-u", script_path] + (extra_args or [])
    logger.info(f"=== Running {name} ===")
    result = subprocess.run(cmd, cwd=os.path.dirname(SCRIPTS_DIR))
    if result.returncode != 0:
        logger.error(f"{name} failed with exit code {result.returncode}")
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Run the full TikTok research pipeline")
    parser.add_argument("--group", default=None, help="Handle group for all steps")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--skip-classify", action="store_true", help="Skip classification")
    parser.add_argument("--skip-download", action="store_true", help="Skip audio download")
    args = parser.parse_args()

    setup_logging()

    group_args = ["--group", args.group] if args.group else []
    config_args = ["--config", args.config]
    common = group_args + config_args

    run_script("pull_videos.py", common)
    run_script("pull_user_info.py", common)

    if not args.skip_download:
        run_script("download_audio.py", common)
        run_script("transcribe_videos.py", common)

    if not args.skip_classify:
        run_script("classify_videos.py", common)
        run_script("classify_accounts.py", common)

    run_script("export_csv.py", common)

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
