-- =============================================================================
-- TikTok research — copy-paste queries for BigQuery CSV download
-- =============================================================================
-- Project:  cfme-mediaengagment-prod
-- Dataset:  tiktok_research
--
-- How to run and download a CSV
-- -----------------------------
-- 1. Open https://console.cloud.google.com/bigquery
-- 2. Sign in with your UT Google account.
-- 3. Select project: cfme-mediaengagment-prod
-- 4. Open the query editor (Compose a new query).
-- 5. Paste ONE query from this file (everything between a "-- QUERY:" header
--    and the next "-- QUERY:" / "-- ---" block). Do not run the whole file.
-- 6. Click Run.
-- 7. When results appear: Save results → CSV (local file).
--      • Browser download is capped at ~10 MB / ~16,000 rows.
--      • For larger pulls: Save results → CSV (Google Drive), up to ~1 GB.
--
-- Always run a COUNT query first so you know how large the download will be.
--
-- Which table to use
-- ------------------
--   content_creators      Pipeline 1 — newsfluencer / creator handles
--   news                  Pipeline 2 — institutional news-account handles
--   keyword               Pipeline 3 — keyword search (includes matched_keywords)
--
-- P1/P2 pulls include video rows AND handle API-failure rows in the same
-- CSV. Failure stubs are listed first. Columns start with collection_status,
-- api_error_code, failure_reason, creator_username, video_id so they are
-- easy to verify. Filter collection_status = 'ok' if you only want videos.
-- On video rows api_error_code is blank and failure_reason is enrichment/OCR.
--
-- Edit the date / handle / keyword literals before running a filtered query.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- QUERY: inventory — row counts by table (run this first)
-- -----------------------------------------------------------------------------
SELECT 'content_creators' AS table_name, COUNT(*) AS rows,
       COUNTIF(collection_status = 'ok') AS video_rows,
       COUNTIF(collection_status = 'api_failed') AS handle_api_failures
FROM `cfme-mediaengagment-prod.tiktok_research.content_creators`
UNION ALL
SELECT 'news', COUNT(*), COUNTIF(collection_status = 'ok'),
       COUNTIF(collection_status = 'api_failed')
FROM `cfme-mediaengagment-prod.tiktok_research.news`
UNION ALL
SELECT 'keyword', COUNT(*), COUNTIF(collection_status = 'ok'),
       COUNTIF(collection_status = 'api_failed')
FROM `cfme-mediaengagment-prod.tiktok_research.keyword`;


-- -----------------------------------------------------------------------------
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
