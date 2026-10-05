
-- Silver layer: schema_bronze.sales_targets_raw -> schema_silver.sales_targets
--
-- KHÁC BIỆT LỚN so với các bảng khác: KHÔNG được dedup theo employee_id
-- đơn thuần, vì 1 nhân viên có NHIỀU DÒNG hợp lệ (1 dòng/tháng x nhiều
-- version kế hoạch). Khoá dedup đúng là composite:
-- (employee_id, plan_version, year, month) - chỉ loại trùng nếu đúng
-- cùng version, cùng tháng (vd do chạy loader lặp lại), KHÔNG loại các
-- version khác nhau.
--
-- Theo profiling: có 5 dòng null ở gần hết các cột nghiệp vụ (0.07%)
-- -> đây là dòng "rác" (dòng trống/dòng tổng chưa lọc hết ở Bronze) ->
-- loại bỏ bằng điều kiện WHERE employee_id IS NOT NULL.
-- =====================================================================

DROP TABLE IF EXISTS schema_silver.sales_targets;

CREATE TABLE schema_silver.sales_targets AS
WITH cleaned AS (
    -- Loại dòng rác: thiếu employee_id (5 dòng theo profiling)
    SELECT *
    FROM schema_bronze.sales_targets_raw
    WHERE employee_id IS NOT NULL AND TRIM(employee_id) <> ''
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(employee_id), TRIM(plan_version), TRIM(year), TRIM(month)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM cleaned
)
SELECT
    TRIM(plan_version)                                          AS plan_version,
    TO_DATE(NULLIF(TRIM(version_date), ''), 'YYYY-MM-DD')       AS version_date,
    TO_DATE(NULLIF(TRIM(effective_from), ''), 'YYYY-MM-DD')     AS effective_from,
    TO_DATE(NULLIF(TRIM(effective_to), ''), 'YYYY-MM-DD')       AS effective_to,
    TRIM(employee_id)                                           AS employee_id,
    TRIM(employee_name)                                         AS employee_name,
    UPPER(TRIM(region))                                         AS region,
    TRIM(team)                                                  AS team,
    NULLIF(TRIM(year), '')::INT                                 AS year,
    NULLIF(TRIM(month), '')::INT                                AS month,
    NULLIF(TRIM(target_revenue), '')::NUMERIC                   AS target_revenue,
    NULLIF(TRIM(target_quantity), '')::NUMERIC                  AS target_quantity,
    NULLIF(TRIM(target_new_customers), '')::NUMERIC             AS target_new_customers,
    _source_file, _sheet_name, _source_platform, _ingested_at, _batch_id
FROM dedup
WHERE rn = 1;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_silver.stg_sales_targets; -- kỳ vọng gần 7283 (trừ 5 dòng rác)
-- SELECT plan_version, COUNT(*) FROM schema_silver.stg_sales_targets GROUP BY plan_version; -- xác nhận đủ các version (v1, v2, v3...)
-- SELECT employee_id, plan_version, year, month, COUNT(*)
--   FROM schema_silver.stg_sales_targets
--   GROUP BY 1,2,3,4 HAVING COUNT(*) > 1; -- kỳ vọng 0 dòng
