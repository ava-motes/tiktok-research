"""Classify account types using OpenAI GPT-4o-mini."""

import json
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)

ACCOUNT_TYPES = {
    1: "politician",
    2: "institutional newsroom",
    3: "fan account",
    4: "backup account",
    5: "not us-based",
    6: "unsure",
    0: "other",
}


def classify_accounts_batch(client: OpenAI, accounts: list, model: str = "gpt-4o-mini",
                             temperature: float = 0) -> list:
    """Classify a batch of accounts by type.

    Args:
        client: OpenAI client instance.
        accounts: List of dicts with user profile fields.
        model: OpenAI model to use.
        temperature: Sampling temperature.

    Returns:
        List of integer codes (one per account).
    """
    items = []
    for i, a in enumerate(accounts):
        items.append(
            f"[{i}] Handle: {a['username']}\n"
            f"Display name: {a.get('display_name', '')}\n"
            f"Bio: {a.get('bio', '')}\n"
            f"Verified: {a.get('is_verified', False)}\n"
            f"Followers: {a.get('follower_count', 0)}\n"
            f"Following: {a.get('following_count', 0)}\n"
            f"Video count: {a.get('video_count', 0)}"
        )

    prompt = "\n\n".join(items)

    resp = client.chat.completions.create(
        model=model,
        temperature=temperature,
        max_tokens=512,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are classifying TikTok accounts into categories. "
                    "For each account, assign exactly ONE type code based on the profile info "
                    "and your knowledge of who they are:\n\n"
                    "1 = politician (elected officials, candidates, government office holders)\n"
                    "2 = institutional newsroom (news organizations, media outlets, professional journalism brands)\n"
                    "3 = fan account (unofficial account about someone else, fan-run)\n"
                    "4 = backup account (secondary/alt account for a creator)\n"
                    "5 = not US-based (primarily based outside the United States)\n"
                    "6 = unsure (cannot determine)\n"
                    "0 = other (independent creators, commentators, influencers, brands, etc.)\n\n"
                    "IMPORTANT: Only use code 2 for actual institutional newsrooms like CNN, NewsNation, etc. "
                    "Individual journalists and creators are code 0.\n\n"
                    "Return ONLY a JSON array of integers, one per account, in order.\n"
                    "Example: [1, 0, 2, 5]"
                ),
            },
            {"role": "user", "content": prompt},
        ],
    )

    text = resp.choices[0].message.content.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    return json.loads(text)
