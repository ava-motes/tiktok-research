-- Copy-paste BigQuery queries. Project: cfme-mediaengagment-prod
-- Dataset: tiktok_research
-- Paste ONE query at a time. Filter collection_status = 'ok' for videos only.
-- Browser CSV download is capped; use Drive for large pulls.

-- QUERY: Pipeline 1 — content creators (videos + failed handles)
-- api_failed rows are first. Filter collection_status = 'ok' for videos only.
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
FROM `cfme-mediaengagment-prod.tiktok_research.content_creators`
ORDER BY CASE WHEN collection_status = 'api_failed' THEN 0 ELSE 1 END,
         collection_date DESC, posted_at DESC;


-- -----------------------------------------------------------------------------
-- QUERY: Pipeline 1 — content creators for one Chicago research date
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
FROM `cfme-mediaengagment-prod.tiktok_research.content_creators`
WHERE collection_date = '2026-08-25'
ORDER BY CASE WHEN collection_status = 'api_failed' THEN 0 ELSE 1 END,
         creator_username, posted_at;


-- -----------------------------------------------------------------------------
-- QUERY: Pipeline 1 — failed-handle checklist only (same rows as above,
-- slim columns). Use this if you want a short verify list, not the full pull.
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
FROM `cfme-mediaengagment-prod.tiktok_research.content_creators`
WHERE collection_status = 'api_failed'
ORDER BY collection_date DESC, creator_username;


-- -----------------------------------------------------------------------------
-- QUERY: videos from one creator (Pipeline 1). Swap the table to news if needed.
-- Change 'underthedesknews' to the handle (no @). Includes an api_failed row
-- if that handle's video/query failed for a date.
-- -----------------------------------------------------------------------------
SELECT
  collection_status,
  api_error_code,
  failure_reason,
  creator_username,
  video_id,
  collection_date,
  video_url,
  posted_at,
  caption,
  likes,
  view_count,
  voice_to_text,
  whisper_transcript,
  ocr_text
FROM `cfme-mediaengagment-prod.tiktok_research.content_creators`
WHERE creator_username = 'underthedesknews'
ORDER BY CASE WHEN collection_status = 'api_failed' THEN 0 ELSE 1 END,
         posted_at DESC;
