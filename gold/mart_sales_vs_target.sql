-- =====================================================================
-- mart_sales_vs_target.sql
-- Gold layer: fact_sales + fact_targets -> schema_gold.mart_sales_vs_target
-- Mục đích: bảng tổng hợp SẴN SÀNG cho Power BI, so sánh doanh số thật
-- (actual) với chỉ tiêu (target) theo từng nhân viên - từng tháng.
--
-- XỬ LÝ MULTI-VERSION: 1 nhân viên - 1 tháng có thể có NHIỀU version
-- target (v1, v2, v3...) trong fact_targets. Mart này CHỈ lấy version
-- MỚI NHẤT (theo version_date, rồi tới plan_version nếu trùng ngày) để
-- so sánh — đúng nghiệp vụ thực tế: khi có target mới, target cũ coi như
-- không còn dùng để đánh giá KPI nữa (nhưng vẫn được LƯU ĐẦY ĐỦ ở
-- fact_targets để tra cứu lịch sử điều chỉnh khi cần).
-- =====================================================================

DROP TABLE IF EXISTS schema_gold.mart_sales_vs_target;

CREATE TABLE schema_gold.mart_sales_vs_target AS
WITH latest_target AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY employee_sk, year, month
            ORDER BY version_date DESC NULLS LAST, plan_version DESC
        ) AS rn
    FROM schema_gold.fact_targets
    WHERE employee_sk IS NOT NULL
),
target_final AS (
    SELECT employee_sk, year, month, region, team, plan_version,
           target_revenue, target_quantity, target_new_customers
    FROM latest_target
    WHERE rn = 1
),
actual_sales AS (
    SELECT
        f.employee_sk,
        d.year,
        d.month,
        SUM(f.net_amount)                    AS actual_revenue,
        SUM(f.quantity)                      AS actual_quantity,
        COUNT(DISTINCT f.customer_sk)        AS actual_customers
    FROM schema_gold.fact_sales f
    JOIN schema_gold.dim_date d ON f.date_id = d.date_id
    WHERE f.employee_sk IS NOT NULL
    GROUP BY f.employee_sk, d.year, d.month
),
actual_returns AS (
    -- MỚI THÊM: tổng giá trị hàng trả theo nhân viên - năm - tháng
    SELECT
        r.employee_sk,
        d.year,
        d.month,
        SUM(r.return_amount) AS total_return_amount
    FROM schema_gold.fact_returns r
    JOIN schema_gold.dim_date d ON r.date_id = d.date_id
    WHERE r.employee_sk IS NOT NULL
    GROUP BY r.employee_sk, d.year, d.month
),
combined AS (
    -- FULL OUTER JOIN target với sales trước (như thiết kế cũ) để không
    -- bỏ sót tháng có target nhưng không bán được gì, hoặc bán được
    -- nhưng không có target tháng đó
    SELECT
        COALESCE(t.employee_sk, a.employee_sk)   AS employee_sk,
        COALESCE(t.year, a.year)                 AS year,
        COALESCE(t.month, a.month)               AS month,
        t.plan_version,
        t.region,
        t.team,
        t.target_revenue,
        t.target_quantity,
        a.actual_revenue,
        a.actual_quantity,
        a.actual_customers
    FROM target_final t
    FULL OUTER JOIN actual_sales a
        ON t.employee_sk = a.employee_sk AND t.year = a.year AND t.month = a.month
)
SELECT
    c.employee_sk,
    de.employee_id,
    de.full_name,
    c.year,
    c.month,
    c.plan_version,
    COALESCE(c.region, de.region)             AS region,
    COALESCE(c.team, de.team)                 AS team,

    COALESCE(c.target_revenue, 0)             AS target_revenue,
    COALESCE(c.actual_revenue, 0)       AS gross_actual_revenue,   -- doanh thu bán hàng thuần (trước return) - giữ lại để đối chiếu
    COALESCE(ar.total_return_amount, 0)       AS actual_return_amount,  -- MỚI: tổng giá trị trả hàng trong kỳ
    COALESCE(c.actual_revenue, 0) - COALESCE(ar.total_return_amount, 0) AS actual_revenue,  -- SỬA: doanh thu ròng = bán - trả

    (COALESCE(c.actual_revenue, 0) - COALESCE(ar.total_return_amount, 0)) - COALESCE(c.target_revenue, 0) AS revenue_gap,
    CASE
        WHEN COALESCE(c.target_revenue, 0) = 0 THEN NULL
        ELSE ROUND(
            (COALESCE(c.actual_revenue, 0) - COALESCE(ar.total_return_amount, 0)) / c.target_revenue * 100
        , 2)
    END                                        AS revenue_achievement_pct,

    COALESCE(c.target_quantity, 0)            AS target_quantity,
    COALESCE(c.actual_quantity, 0)            AS actual_quantity,
    COALESCE(c.actual_customers, 0)           AS actual_customers,

    NOW() AS _loaded_at

FROM combined c
LEFT JOIN actual_returns ar
    ON c.employee_sk = ar.employee_sk AND c.year = ar.year AND c.month = ar.month
LEFT JOIN schema_gold.dim_employees de
    ON de.employee_sk = c.employee_sk;
-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.mart_sales_vs_target;
-- SELECT * FROM schema_gold.mart_sales_vs_target WHERE target_revenue = 0 AND actual_revenue > 0; -- nhân viên có bán hàng nhưng không có target tháng đó (bất thường, đáng kiểm tra)
-- SELECT employee_id, year, month, revenue_achievement_pct
--   FROM schema_gold.mart_sales_vs_target ORDER BY revenue_achievement_pct DESC LIMIT 10; -- top nhân viên vượt target nhiều nhất
