"""Pipeline 2 — news / journalism accounts.

Not production. The news-account list and NEWS_API credentials are not confirmed.
Do not run this collector until Pipeline 1 is approved and the list is provided.
"""

from __future__ import annotations

import sys


def main() -> int:
    print(
        "Pipeline 2 (news_accounts) is not implemented for production yet.\n"
        "The news/journalism account list is not confirmed.\n"
        "Finish and approve Pipeline 1 before implementing Pipeline 2.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
