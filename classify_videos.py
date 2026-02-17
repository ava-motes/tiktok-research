"""Classify videos as news/politics using OpenAI, then update follower CSV with counts."""

import csv
import json
import time
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

VIDEOS_CSV = "sample_videos_by_handle.csv"
FOLLOWERS_CSV = "sample_handle_info.csv"
BATCH_SIZE = 20  # videos per API call to save on requests


def classify_batch(videos):
    """Send a batch of videos to GPT-4o-mini and get news/politics labels.

    Each video is a dict with at least 'caption' and 'hashtags'.
    Returns a list of {"news": 0|1, "politics": 0|1} in the same order.
    """
    items = []
    for i, v in enumerate(videos):
        items.append(f"[{i}] Caption: {v['caption']}\nHashtags: {v['hashtags']}")

    prompt = "\n\n".join(items)

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        max_tokens=1024,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are classifying TikTok videos. For each video, decide:\n"
                    "1. Is it about NEWS? (current events, breaking news, news commentary, reporting) → 1 or 0\n"
                    "2. Is it about POLITICS? (government, elections, politicians, policy, political commentary) → 1 or 0\n\n"
                    "Return ONLY a JSON array with one object per video, in order. "
                    "Each object has keys \"news\" and \"politics\" with integer values 0 or 1.\n"
                    "Example: [{\"news\": 1, \"politics\": 1}, {\"news\": 0, \"politics\": 0}]"
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
    )

    text = resp.choices[0].message.content.strip()
    # Extract JSON array from response (handle markdown code blocks)
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    results = json.loads(text)
    return results


def main():
    # Read all videos
    with open(VIDEOS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        videos = list(reader)

    print(f"Loaded {len(videos)} videos from {VIDEOS_CSV}")

    # Classify in batches
    all_labels = []
    for i in range(0, len(videos), BATCH_SIZE):
        batch = videos[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(videos) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Classifying batch {batch_num}/{total_batches} ({len(batch)} videos)...")

        try:
            labels = classify_batch(batch)
            all_labels.extend(labels)
        except Exception as e:
            print(f"  Error on batch {batch_num}: {e}")
            # Default to 0/0 for failed batches
            all_labels.extend([{"news": 0, "politics": 0}] * len(batch))

        # Small delay to avoid rate limits
        if i + BATCH_SIZE < len(videos):
            time.sleep(0.5)

    # Write updated videos CSV
    out_fields = fieldnames + ["news", "politics"]
    with open(VIDEOS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        for video, label in zip(videos, all_labels):
            video["news"] = label.get("news", 0)
            video["politics"] = label.get("politics", 0)
            writer.writerow(video)

    print(f"Updated {VIDEOS_CSV} with news/politics columns")

    # Aggregate counts per handle
    handle_stats = {}
    for video in videos:
        handle = video["handle"]
        if handle not in handle_stats:
            handle_stats[handle] = {"total_videos": 0, "news_videos": 0, "politics_videos": 0}
        handle_stats[handle]["total_videos"] += 1
        handle_stats[handle]["news_videos"] += int(video.get("news", 0))
        handle_stats[handle]["politics_videos"] += int(video.get("politics", 0))

    # Update follower CSV with counts
    with open(FOLLOWERS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        follower_fields = reader.fieldnames
        followers = list(reader)

    new_cols = ["total_videos", "news_videos", "politics_videos"]
    out_follower_fields = [f for f in follower_fields if f not in new_cols] + new_cols
    with open(FOLLOWERS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_follower_fields, extrasaction="ignore")
        writer.writeheader()
        for row in followers:
            handle = row["handle"]
            stats = handle_stats.get(handle, {"total_videos": 0, "news_videos": 0, "politics_videos": 0})
            row["total_videos"] = stats["total_videos"]
            row["news_videos"] = stats["news_videos"]
            row["politics_videos"] = stats["politics_videos"]
            writer.writerow(row)

    print(f"Updated {FOLLOWERS_CSV} with total_videos/news_videos/politics_videos columns")
    print("Done!")


if __name__ == "__main__":
    main()
