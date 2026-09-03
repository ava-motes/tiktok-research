"""Deprecated filename. Use scripts/collect_news.py."""

from __future__ import annotations

import os
import sys

print("NOTE: use scripts/collect_news.py", file=sys.stderr)
target = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collect_news.py")
os.execv(sys.executable, [sys.executable, target, *sys.argv[1:]])
