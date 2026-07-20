-- SAFE DROP for deprecated BigQuery tables
-- ONLY run after scripts/migrate_bq_simplified_architecture.py validates successfully.
-- These tables are NOT used by the v3 enrichment pipeline.
--
-- Project: cfme-mediaengagment-prod
-- Dataset: tiktok_research

-- Preview (optional):
-- SELECT table_id, row_count, size_bytes
-- FROM `cfme-mediaengagment-prod.tiktok_research.__TABLES__`
-- WHERE table_id IN ('videos_raw','video_transcripts','video_ocr','video_emojis');

DROP TABLE IF EXISTS `cfme-mediaengagment-prod.tiktok_research.videos_raw`;
DROP TABLE IF EXISTS `cfme-mediaengagment-prod.tiktok_research.video_transcripts`;
DROP TABLE IF EXISTS `cfme-mediaengagment-prod.tiktok_research.video_ocr`;
DROP TABLE IF EXISTS `cfme-mediaengagment-prod.tiktok_research.video_emojis`;

-- Keep:
--   tiktok_video_enriched
--   tiktok_pipeline_logs
--   v_enrichment_*
