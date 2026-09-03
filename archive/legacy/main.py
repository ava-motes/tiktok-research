"""TikTok Research API v2 — pull videos and follower data for tracked handles."""

from pull_videos import main as pull_videos_main
from pull_followers import main as pull_followers_main


if __name__ == "__main__":
    print("=== Pulling videos by handle (1/1/2026 – today) ===")
    pull_videos_main()

    print("\n=== Pulling follower counts ===")
    pull_followers_main()

    print("\nAll done! Check sample_videos_by_handle.csv and sample_handle_info.csv")
