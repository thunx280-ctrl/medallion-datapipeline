
-- (Tên file giữ nguyên "stg_territoty.sql" khớp với tên bạn đã đặt sẵn
-- trong staging/ - lưu ý tên có thể là lỗi chính tả của "territory")
-- Silver layer: schema_bronze.territory_mapping -> schema_silver.stg_territory_mapping
--
-- territory_id: 1843/1843 không trùng -> khoá dedup
-- =====================================================================

DROP TABLE IF EXISTS schema_silver.territory_mapping;

CREATE TABLE schema_silver.territory_mapping AS
WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(territory_id)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM schema_bronze.territory_mapping
    WHERE territory_id IS NOT NULL AND TRIM(territory_id) <> ''
)
SELECT
    TRIM(territory_id)                                          AS territory_id,
    TRIM(employee_id)                                           AS employee_id,
    TRIM(customer_id)                                           AS customer_id,
    UPPER(TRIM(region))                                         AS region,
    TRIM(team)                                                  AS team,
    TO_DATE(NULLIF(TRIM(effective_date), ''), 'YYYY-MM-DD')     AS effective_date,
    TO_DATE(NULLIF(TRIM(expiry_date), ''), 'YYYY-MM-DD')        AS expiry_date,
    TRIM(version)                                               AS version,
    _source_file, _source_platform, _ingested_at, _batch_id
FROM dedup
WHERE rn = 1;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_silver.stg_territory_mapping; -- kỳ vọng = 1843
-- SELECT COUNT(*) FROM schema_silver.stg_territory_mapping WHERE customer_id IS NULL;
