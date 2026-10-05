-- =====================================================================
-- fact_targets.sql
-- Gold layer: schema_silver.sales_targets -> schema_gold.fact_targets
-- Grain: 1 dòng = 1 nhân viên - 1 tháng - 1 PHIÊN BẢN kế hoạch (plan_version)
--
-- QUAN TRỌNG: KHÔNG lọc chỉ lấy version mới nhất ở đây — giữ NGUYÊN mọi
-- version (đúng yêu cầu ban đầu "không ghi đè version cũ"). Việc chọn
-- version nào để so sánh với doanh số thật (thường là version MỚI NHẤT
-- có hiệu lực tại thời điểm đó) sẽ xử lý ở mart_sales_vs_target.sql.
--
-- date_id: targets đo theo THÁNG chứ không theo ngày -> quy ước lấy
-- NGÀY ĐẦU THÁNG (vd tháng 3/2024 -> date_id = 20240301) làm đại diện,
-- join được với dim_date bình thường (dim_date có đủ mọi ngày trong tháng).
--
-- employee_sk: join AS-OF (point-in-time) giống fact_sales, dùng
-- effective_from của target (ngày kế hoạch này bắt đầu áp dụng) làm mốc
-- để xác định nhân viên đó đang ở version thông tin nào tại thời điểm đó.
-- =====================================================================

DROP TABLE IF EXISTS schema_gold.fact_targets;

CREATE TABLE schema_gold.fact_targets AS
SELECT
    t.plan_version,
    t.version_date,
    t.effective_from,
    t.effective_to,

    (TO_CHAR(MAKE_DATE(t.year, t.month, 1), 'YYYYMMDD'))::INT  AS date_id,  -- ngày đầu tháng của kỳ target
    t.year,
    t.month,

    e.employee_sk,   -- có thể NULL nếu employee_id không khớp / ngoài khoảng hiệu lực -> xem CHECKPOINT

    -- Degenerate attributes giữ lại để filter nhanh không cần join dim
    t.region,
    t.team,

    -- Measures
    t.target_revenue,
    t.target_quantity,
    t.target_new_customers,

    NOW() AS _loaded_at

FROM schema_silver.sales_targets t
LEFT JOIN schema_gold.dim_employees e
    ON t.employee_id = e.employee_id
   AND COALESCE(t.effective_from, t.version_date) >= e.effective_date
   AND (COALESCE(t.effective_from, t.version_date) <= e.expiry_date OR e.expiry_date IS NULL);

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.fact_targets; -- kỳ vọng khớp số dòng stg_sales_targets
-- SELECT plan_version, COUNT(*) FROM schema_gold.fact_targets GROUP BY plan_version; -- xác nhận đủ mọi version
-- SELECT COUNT(*) FROM schema_gold.fact_targets WHERE employee_sk IS NULL; -- nếu > 0, kiểm tra employee_id đó
-- SELECT COUNT(*) FROM schema_gold.fact_targets WHERE date_id IS NULL; -- kỳ vọng = 0 (year/month lỗi sẽ khiến MAKE_DATE lỗi)
