-- Silver layer: schema_bronze.distributors -> schema_silver.stg_distributors

-- distributor_id: 138/138 không trùng -> khoá dedup
-- Khác customers: tax_code ở đây 0% null (nhà phân phối là doanh nghiệp,
-- luôn có mã số thuế) -> không cần NULLIF đặc biệt như customers


DROP TABLE IF EXISTS schema_silver.distributors;

CREATE TABLE schema_silver.distributors AS
WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(distributor_id)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM schema_bronze.distributors
    WHERE distributor_id IS NOT NULL AND TRIM(distributor_id) <> ''
)
SELECT
    TRIM(distributor_id)                                        AS distributor_id,
    TRIM(distributor_name)                                      AS distributor_name,
    UPPER(TRIM(tier))                                            AS tier,
    UPPER(TRIM(channel))                                         AS channel,
    TRIM(province)                                               AS province,
    UPPER(TRIM(region))                                          AS region,
    TRIM(contact_person)                                         AS contact_person,
    REGEXP_REPLACE(TRIM(phone), '[^0-9+]', '', 'g')             AS phone,
    LOWER(TRIM(email))                                           AS email,
    NULLIF(TRIM(tax_code), '')                                   AS tax_code,
    TO_DATE(NULLIF(TRIM(join_date), ''), 'YYYY-MM-DD')          AS join_date,
    NULLIF(TRIM(credit_limit), '')::NUMERIC                      AS credit_limit,
    INITCAP(TRIM(status))                                        AS status,
    TRIM(assigned_supervisor_id)                                 AS assigned_supervisor_id,
    _source_file, _source_platform, _ingested_at, _batch_id
FROM dedup
WHERE rn = 1;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_silver.stg_distributors; -- kỳ vọng = 138
-- SELECT COUNT(*) FROM schema_silver.stg_distributors WHERE join_date IS NULL;
