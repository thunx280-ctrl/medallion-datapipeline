
-- Silver layer: schema_bronze.return_transactions -> schema_silver.return_transactions
-- return_id: 3665/3665 không trùng -> khoá dedup
-- =====================================================================

DROP TABLE IF EXISTS schema_silver.return_transactions;

CREATE TABLE schema_silver.return_transactions AS
WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(return_id)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM schema_bronze.return_transactions
    WHERE return_id IS NOT NULL AND TRIM(return_id) <> ''
)
SELECT
    TRIM(return_id)                                            AS return_id,
    TRIM(original_order_id)                                    AS original_order_id,
    TO_DATE(NULLIF(TRIM(return_date), ''), 'YYYY-MM-DD')       AS return_date,
    NULLIF(TRIM(return_month), '')::INT                        AS return_month,
    TRIM(customer_id)                                          AS customer_id,
    TRIM(employee_id)                                          AS employee_id,
    TRIM(product_id)                                           AS product_id,
    UPPER(TRIM(region))                                        AS region,
    TRIM(province)                                             AS province,
    NULLIF(TRIM(return_quantity), '')::NUMERIC                 AS return_quantity,
    NULLIF(TRIM(unit_price), '')::NUMERIC                      AS unit_price,
    NULLIF(TRIM(return_amount), '')::NUMERIC                   AS return_amount,
    INITCAP(TRIM(return_reason))                               AS return_reason,
    INITCAP(TRIM(status))                                      AS status,
    _source_file, _source_platform, _ingested_at, _batch_id
FROM dedup
WHERE rn = 1;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_silver.stg_return_transactions; -- kỳ vọng = 3665
-- SELECT COUNT(*) FROM schema_silver.stg_return_transactions WHERE return_amount IS NULL;
