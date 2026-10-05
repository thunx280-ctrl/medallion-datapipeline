DROP TABLE IF EXISTS schema_gold.dim_date;

CREATE TABLE schema_gold.dim_date AS
WITH date_series AS (
    SELECT generate_series(
        '2018-01-01'::DATE,
        '2030-12-31'::DATE,
        '1 day'::INTERVAL
    )::DATE AS full_date
)
SELECT
    -- Khoá chính: YYYYMMDD dạng số nguyên (vd 20240315)
    (TO_CHAR(full_date, 'YYYYMMDD'))::INT                  AS date_id,

    full_date,

    EXTRACT(YEAR FROM full_date)::INT                      AS year,
    EXTRACT(QUARTER FROM full_date)::INT                   AS quarter,
    EXTRACT(MONTH FROM full_date)::INT                     AS month,
    EXTRACT(DAY FROM full_date)::INT                       AS day,
    EXTRACT(WEEK FROM full_date)::INT                      AS week_of_year,

    -- ISO: Thứ 2 = 1 ... Chủ nhật = 7 (khác chuẩn Mỹ Chủ nhật = 0)
    EXTRACT(ISODOW FROM full_date)::INT                    AS day_of_week,
    TO_CHAR(full_date, 'Day')                              AS day_name,
    TO_CHAR(full_date, 'Month')                            AS month_name,
    TO_CHAR(full_date, 'YYYY-MM')                          AS year_month,      -- vd "2024-03", tiện GROUP BY theo tháng
    'Q' || EXTRACT(QUARTER FROM full_date) || '-' || EXTRACT(YEAR FROM full_date) AS year_quarter, -- vd "Q1-2024"

    -- Thứ 7, Chủ nhật -> cuối tuần (ISODOW 6=Thứ 7, 7=Chủ nhật)
    (EXTRACT(ISODOW FROM full_date) IN (6, 7))              AS is_weekend,

    (full_date = DATE_TRUNC('month', full_date)::DATE)                          AS is_first_day_of_month,
    (full_date = (DATE_TRUNC('month', full_date) + INTERVAL '1 month - 1 day')::DATE) AS is_last_day_of_month

FROM date_series
ORDER BY full_date;

ALTER TABLE schema_gold.dim_date ADD PRIMARY KEY (date_id);

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.dim_date; -- kỳ vọng = số ngày từ 2018-01-01 đến 2030-12-31 (~4748 dòng)
-- SELECT * FROM schema_gold.dim_date WHERE full_date = '2024-03-15'; -- kiểm tra 1 ngày cụ thể ra đúng date_id = 20240315
-- SELECT day_name, COUNT(*) FROM schema_gold.dim_date GROUP BY day_name; -- mỗi thứ phải có số dòng gần bằng nhau
