"""Pipeline 3 — keyword search.

Not production. Do not run the 263-term list until Pipeline 1 and 2 are approved.
"""

from __future__ import annotations

import sys


def main() -> int:
    print(
        "Pipeline 3 (keyword_search) is not implemented for production yet.\n"
        "Do not run keyword collection until Pipeline 1 is approved.\n"
        "The canonical keyword file remains config/keywords/mediacloud_march_2026.txt.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
