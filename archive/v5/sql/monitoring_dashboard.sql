-- Monitoring views for final tiktok_video_enriched schema (v4)
CREATE OR REPLACE VIEW `cfme-mediaengagment-prod.tiktok_research.v_enrichment_daily` AS
SELECT
  enrichment_date AS day,
  COUNT(*) AS videos_processed,
  COUNTIF(enrichment_status IN ('ok', 'partial')) AS enriched_ok_partial,
  COUNTIF(LENGTH(IFNULL(whisper_transcript, '')) > 0) AS whisper_ok,
  COUNTIF(LENGTH(IFNULL(ocr_text, '')) > 0) AS ocr_ok,
  COUNTIF(LENGTH(IFNULL(emoji_characters, '')) > 0) AS videos_with_emoji,
  ROUND(100 * COUNTIF(LENGTH(IFNULL(whisper_transcript, '')) > 0) / COUNT(*), 2) AS whisper_success_pct,
  ROUND(100 * COUNTIF(LENGTH(IFNULL(ocr_text, '')) > 0) / COUNT(*), 2) AS ocr_success_pct
FROM `cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched`
WHERE enrichment_date IS NOT NULL AND enrichment_date != ''
GROUP BY enrichment_date
ORDER BY enrichment_date DESC;

CREATE OR REPLACE VIEW `cfme-mediaengagment-prod.tiktok_research.v_enrichment_today` AS
SELECT * FROM `cfme-mediaengagment-prod.tiktok_research.v_enrichment_daily`
WHERE day = FORMAT_DATE('%Y-%m-%d', CURRENT_DATE());

CREATE OR REPLACE VIEW `cfme-mediaengagment-prod.tiktok_research.v_enrichment_failures` AS
SELECT
  enrichment_date AS day,
  enrichment_status AS failure_bucket,
  COUNT(*) AS n
FROM `cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched`
WHERE enrichment_status IN ('failed', 'partial')
GROUP BY enrichment_date, enrichment_status
ORDER BY day DESC, n DESC;

CREATE OR REPLACE VIEW `cfme-mediaengagment-prod.tiktok_research.v_enrichment_quality` AS
SELECT
  enrichment_date AS day,
  enrichment_status,
  COUNT(*) AS n,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY enrichment_date), 2) AS pct
FROM `cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched`
GROUP BY enrichment_date, enrichment_status
ORDER BY day DESC;

CREATE OR REPLACE VIEW `cfme-mediaengagment-prod.tiktok_research.v_enrichment_duplicates` AS
SELECT video_id, COUNT(*) AS row_count
FROM `cfme-mediaengagment-prod.tiktok_research.tiktok_video_enriched`
GROUP BY video_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC;
