"""Classify videos as news and politics using OpenAI GPT-4o-mini."""

import json
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)


def classify_batch(client: OpenAI, videos: list, model: str = "gpt-4o-mini",
                   temperature: float = 0) -> list:
    """Classify a batch of videos for news and politics separately.

    Returns:
        List of {"news": 0|1, "politics": 0|1} in the same order.
    """
    items = []
    for i, v in enumerate(videos):
        items.append(f"[{i}] Caption: {v['caption']}\nHashtags: {v['hashtags']}")

    prompt = "\n\n".join(items)

    resp = client.chat.completions.create(
        model=model,
        temperature=temperature,
        max_tokens=2048,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are classifying TikTok videos. For each video, decide two things:\n\n"
                    "1. NEWS (1 or 0): Is it about news? This includes current events, breaking news, "
                    "news reporting, news commentary, or journalism about real-world events.\n\n"
                    "2. POLITICS (1 or 0): Is it about politics? This includes government, elections, "
                    "politicians, policy, political commentary, political satire, or political activism.\n\n"
                    "A video can be both (e.g. a news report about an election), one, or neither.\n\n"
                    "Return ONLY a JSON array with one object per video, in order. "
                    "Each object has keys \"news\" and \"politics\" with integer values 0 or 1.\n"
                    "Example: [{\"news\": 1, \"politics\": 1}, {\"news\": 0, \"politics\": 0}]"
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
