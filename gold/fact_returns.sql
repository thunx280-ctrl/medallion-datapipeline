-- =====================================================================
-- fact_returns.sql
-- Gold layer: schema_silver.return_transactions -> schema_gold.fact_returns
-- Grain: 1 dòng = 1 return_id (đã xác nhận unique ở Silver)
-- Cùng nguyên tắc join AS-OF cho dim_employees như fact_sales/fact_targets.
-- =====================================================================

DROP TABLE IF EXISTS schema_gold.fact_returns;

CREATE TABLE schema_gold.fact_returns AS
SELECT
    r.return_id,
    r.original_order_id,

    (TO_CHAR(r.return_date, 'YYYYMMDD'))::INT AS date_id,
    c.customer_sk,
    p.product_sk,
    e.employee_sk,

    r.region,
    r.province,
    r.return_reason,
    r.status,

    r.return_quantity,
    r.unit_price,
    r.return_amount,

    NOW() AS _loaded_at

FROM schema_silver.return_transactions r
LEFT JOIN schema_gold.dim_customers c
    ON r.customer_id = c.customer_id
LEFT JOIN schema_gold.dim_products p
    ON r.product_id = p.product_id
LEFT JOIN schema_gold.dim_employees e
    ON r.employee_id = e.employee_id
   AND r.return_date >= e.effective_date
   AND (r.return_date <= e.expiry_date OR e.expiry_date IS NULL);

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.fact_returns; -- kỳ vọng = 3665 (khớp stg_return_transactions)
-- SELECT COUNT(*) FROM schema_gold.fact_returns WHERE customer_sk IS NULL;
-- SELECT COUNT(*) FROM schema_gold.fact_returns WHERE product_sk IS NULL;
-- SELECT SUM(return_amount) FROM schema_gold.fact_returns; -- tổng giá trị hàng trả
-- -- So sánh return_amount thật với return_quantity * unit_price -> nếu lệch nhiều, báo lại (có thể do chiết khấu/phí xử lý khác)
-- SELECT COUNT(*) FROM schema_gold.fact_returns WHERE ABS(return_amount - (return_quantity * unit_price)) > 1;
