
-- Silver layer: schema_bronze.employees -> schema_silver.employees
--
-- employee_id: 114/114 không trùng -> khoá dedup.
-- resign_date, transfer_note: 100% null hiện tại (bình thường - đây là
-- snapshot đầu tiên, chưa ai nghỉ việc/chuyển team) -> giữ NULL, không ép giá trị.
-- Lưu ý: logic SCD Type 2 THẬT SỰ (đóng bản ghi cũ khi nhân viên đổi vùng)
-- sẽ xử lý ở Gold layer (dim_employees), KHÔNG xử lý ở Silver này.
-- =====================================================================

DROP TABLE IF EXISTS schema_silver.employees;

CREATE TABLE schema_silver.employees AS
WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(employee_id)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM schema_bronze.employees
    WHERE employee_id IS NOT NULL AND TRIM(employee_id) <> ''
)
SELECT
    TRIM(employee_id)                                             AS employee_id,
    TRIM(full_name)                                               AS full_name,
    UPPER(TRIM(gender))                                           AS gender,
    TO_DATE(NULLIF(TRIM(date_of_birth), ''), 'YYYY-MM-DD')        AS date_of_birth,
    TO_DATE(NULLIF(TRIM(join_date), ''), 'YYYY-MM-DD')            AS join_date,
    INITCAP(TRIM(position))                                       AS "position",  -- "position" là từ khoá SQL, cần đặt trong ngoặc kép
    UPPER(TRIM(region))                                           AS region,
    TRIM(team)                                                    AS team,
    LOWER(TRIM(email))                                            AS email,
    REGEXP_REPLACE(TRIM(phone), '[^0-9+]', '', 'g')               AS phone,
    INITCAP(TRIM(status))                                         AS status,
    TRIM(version)                                                 AS version,
    TO_DATE(NULLIF(TRIM(effective_date), ''), 'YYYY-MM-DD')       AS effective_date,
    TO_DATE(NULLIF(TRIM(resign_date), ''), 'YYYY-MM-DD')          AS resign_date,
    NULLIF(TRIM(transfer_note), '')                               AS transfer_note,
    _source_file, _source_platform, _ingested_at, _batch_id
FROM dedup
WHERE rn = 1;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_silver.employees; -- kỳ vọng = 114
-- SELECT COUNT(*) FROM schema_silver.employees WHERE date_of_birth IS NULL;
