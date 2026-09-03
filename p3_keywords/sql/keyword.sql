-- Copy-paste BigQuery queries. Project: cfme-mediaengagment-prod
-- Dataset: tiktok_research
-- Paste ONE query at a time. Filter collection_status = 'ok' for videos only.
-- Browser CSV download is capped; use Drive for large pulls.

-- QUERY: Pipeline 3 — keyword search, all collected videos
-- matched_keywords is joined with " | " so it downloads cleanly as CSV.
-- -----------------------------------------------------------------------------
SELECT
  video_id,
  video_url,
  creator_username,
  creator_display_name,
  posted_at,
  caption,
  hashtags,
  ARRAY_TO_STRING(matched_keywords, ' | ') AS matched_keywords,
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
  collection_date,
  collection_window_start,
  collection_window_end,
  pipeline_id,
  api_source
FROM `cfme-mediaengagment-prod.tiktok_research.keyword`
WHERE collection_status = 'ok'
ORDER BY collection_date DESC, posted_at DESC;


-- -----------------------------------------------------------------------------
-- QUERY: Pipeline 3 — keyword search for one Chicago research date
-- Change '2026-08-28' to the date you need.
-- -----------------------------------------------------------------------------
SELECT
  video_id,
  video_url,
  creator_username,
  posted_at,
  caption,
  hashtags,
  ARRAY_TO_STRING(matched_keywords, ' | ') AS matched_keywords,
  likes,
  comments_count,
  shares,
  favorites,
  view_count,
  voice_to_text,
  whisper_transcript,
  ocr_text,
  collection_date
FROM `cfme-mediaengagment-prod.tiktok_research.keyword`
WHERE collection_status = 'ok'
  AND collection_date = '2026-08-28'
ORDER BY posted_at DESC;


-- -----------------------------------------------------------------------------
-- QUERY: Pipeline 3 — videos that matched one keyword
-- Change 'supreme court ruling' to the term you need (lowercase as stored).
-- -----------------------------------------------------------------------------
SELECT
  video_id,
  video_url,
  creator_username,
  posted_at,
  caption,
  ARRAY_TO_STRING(matched_keywords, ' | ') AS matched_keywords,
  likes,
  view_count,
  voice_to_text,
  collection_date
FROM `cfme-mediaengagment-prod.tiktok_research.keyword`
WHERE collection_status = 'ok'
  AND 'supreme court ruling' IN UNNEST(matched_keywords)
ORDER BY posted_at DESC;


-- -----------------------------------------------------------------------------
