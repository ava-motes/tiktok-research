"""Deprecated filename. Use scripts/validate_keyword.py."""

from __future__ import annotations

import os
import sys

print("NOTE: use scripts/validate_keyword.py", file=sys.stderr)
target = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validate_keyword.py")
os.execv(sys.executable, [sys.executable, target, *sys.argv[1:]])
