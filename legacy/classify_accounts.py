"""Classify each account's type using OpenAI based on profile info."""

import csv
import json
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

FOLLOWERS_CSV = "sample_handle_info.csv"

ACCOUNT_TYPES = {
    1: "politician",
    2: "institutional newsroom",
    3: "fan account",
    4: "backup account",
    5: "not us-based",
    6: "unsure",
    0: "other",
}

BATCH_SIZE = 10


def classify_batch(accounts):
    """Classify a batch of accounts by type."""
    items = []
    for i, a in enumerate(accounts):
        items.append(
            f"[{i}] Handle: {a['handle']}\n"
            f"Display name: {a['display_name']}\n"
            f"Bio: {a['bio']}\n"
            f"Verified: {a['is_verified']}\n"
            f"Followers: {a['follower_count']}\n"
            f"Following: {a['following_count']}\n"
            f"Video count: {a['video_count']}"
        )

    prompt = "\n\n".join(items)

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
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
                    "Return ONLY a JSON array of integers, one per account, in order.\n"
                    "Example: [1, 0, 2, 5]"
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
    )

    text = resp.choices[0].message.content.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    return json.loads(text)


def main():
    with open(FOLLOWERS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Loaded {len(rows)} accounts from {FOLLOWERS_CSV}")

    all_codes = []
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Classifying batch {batch_num}/{total_batches}...")

        try:
            codes = classify_batch(batch)
            all_codes.extend(codes)
        except Exception as e:
            print(f"  Error on batch {batch_num}: {e}")
            all_codes.extend([6] * len(batch))

    # Write back with new columns
    out_fields = fieldnames + ["account_type_code", "account_type_label"]
    with open(FOLLOWERS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        for row, code in zip(rows, all_codes):
            row["account_type_code"] = code
            row["account_type_label"] = ACCOUNT_TYPES.get(code, "unsure")
            writer.writerow(row)

    print(f"\nUpdated {FOLLOWERS_CSV} with account_type_code and account_type_label columns")

    # Print summary
    for row, code in zip(rows, all_codes):
        label = ACCOUNT_TYPES.get(code, "unsure")
        print(f"  {row['handle']:30s} → {code} ({label})")


if __name__ == "__main__":
    main()
