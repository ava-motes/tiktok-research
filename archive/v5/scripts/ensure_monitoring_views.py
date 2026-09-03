#!/usr/bin/env python3
"""Create/replace BigQuery monitoring views from sql/monitoring_dashboard.sql."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tiktok.enrichment.bigquery_loader import bigquery_configured, ensure_dataset_and_tables
from tiktok.logging_setup import setup_logging


def main() -> int:
    setup_logging()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    if not bigquery_configured():
        print("BigQuery not configured", file=sys.stderr)
        return 1
    ensure_dataset_and_tables()
    sql_path = os.path.join(root, "sql", "monitoring_dashboard.sql")
    with open(sql_path, encoding="utf-8") as f:
        sql = f.read()
    # Split on CREATE statements
    from google.cloud import bigquery

    client = bigquery.Client()
    statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
    # Re-join comments-only filtering more carefully
    chunks = []
    buf = []
    for line in sql.splitlines():
        if line.strip().startswith("--") and not buf:
            continue
        buf.append(line)
        if line.strip().endswith(";") and "CREATE" in "\n".join(buf).upper():
            chunks.append("\n".join(buf).rstrip().rstrip(";"))
            buf = []
    if not chunks:
        chunks = statements
    for i, stmt in enumerate(chunks, 1):
        if "CREATE" not in stmt.upper():
            continue
        print(f"Running statement {i}/{len(chunks)}...")
        client.query(stmt).result()
    print("Monitoring views ready:")
    print("  v_enrichment_daily")
    print("  v_enrichment_today")
    print("  v_enrichment_failures")
    print("  v_enrichment_quality")
    print("  v_enrichment_duplicates")
    return 0


if __name__ == "__main__":
    sys.exit(main())
