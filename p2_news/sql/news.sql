-- Copy-paste BigQuery queries. Project: cfme-mediaengagment-prod
-- Dataset: tiktok_research
-- Paste ONE query at a time. Filter collection_status = 'ok' for videos only.
-- Browser CSV download is capped; use Drive for large pulls.

-- QUERY: Pipeline 2 — news / journalism accounts (videos + failed handles)
-- -----------------------------------------------------------------------------
SELECT
  collection_status,
  api_error_code,
  failure_reason,
  creator_username,
  video_id,
  collection_date,
  video_url,
  creator_display_name,
  verified_status,
  follower_count,
  posted_at,
  caption,
  hashtags,
  likes,
  comments_count,
  shares,
  favorites,
  view_count,
  video_duration,
  region_code,
  voice_to_text,
  sticker_text,
  whisper_transcript,
  ocr_text,
  emoji_characters,
  emoji_descriptions,
  collection_window_start,
  collection_window_end,
  pipeline_id,
  api_source
FROM `cfme-mediaengagment-prod.tiktok_research.news`
ORDER BY CASE WHEN collection_status = 'api_failed' THEN 0 ELSE 1 END,
         collection_date DESC, posted_at DESC;


-- -----------------------------------------------------------------------------
-- QUERY: Pipeline 2 — news accounts for one Chicago research date
-- Change '2026-08-25' to the date you need. Includes failed handles.
-- -----------------------------------------------------------------------------
SELECT
  collection_status,
  api_error_code,
  failure_reason,
  creator_username,
  video_id,
  collection_date,
  video_url,
  creator_display_name,
  posted_at,
  caption,
  hashtags,
  likes,
  comments_count,
  shares,
  favorites,
  view_count,
  voice_to_text,
  whisper_transcript,
  ocr_text
FROM `cfme-mediaengagment-prod.tiktok_research.news`
WHERE collection_date = '2026-08-25'
ORDER BY CASE WHEN collection_status = 'api_failed' THEN 0 ELSE 1 END,
         creator_username, posted_at;


-- -----------------------------------------------------------------------------
-- QUERY: Pipeline 2 — handles whose Research API video/query failed
-- -----------------------------------------------------------------------------
SELECT
  collection_date,
  creator_username,
  api_error_code,
  failure_reason,
  video_id,
  collection_window_start,
  collection_window_end,
  api_source,
  pipeline_id
FROM `cfme-mediaengagment-prod.tiktok_research.news`
WHERE collection_status = 'api_failed'
ORDER BY collection_date DESC, creator_username;


-- -----------------------------------------------------------------------------
