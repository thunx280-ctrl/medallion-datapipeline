-- =====================================================================
-- fact_sales.sql
-- Gold layer: schema_silver.sales_transactions -> schema_gold.fact_sales
-- Grain (độ chi tiết 1 dòng): 1 dòng sản phẩm trong 1 đơn hàng
-- (order_id, product_id) — khớp đúng grain đã xác nhận ở Silver.
--
-- fact_sales KHÔNG cần SCD — luôn DROP + CREATE lại toàn bộ mỗi lần chạy,
-- vì bản thân số liệu bán hàng (net_amount, quantity...) không "đổi phiên
-- bản" như thông tin nhân viên — chỉ cần join đúng dimension TẠI THỜI
-- ĐIỂM bán hàng là đủ.
--
-- ĐIỂM QUAN TRỌNG NHẤT của file này: join dim_employees (SCD2) KHÔNG
-- join theo is_current=TRUE (vì đó là nhân viên ĐANG hiện tại, có thể
-- SAI với thời điểm đơn hàng thật sự phát sinh trong quá khứ), mà join
-- "AS-OF" theo order_date nằm trong khoảng [effective_date, expiry_date]
-- của nhân viên — đây gọi là "point-in-time join", nguyên tắc bắt buộc
-- khi fact table join tới 1 dimension SCD2.
-- =====================================================================

DROP TABLE IF EXISTS schema_gold.fact_sales;

CREATE TABLE schema_gold.fact_sales AS
SELECT
    -- Degenerate dimension: order_id không tách thành dim riêng vì không
    -- có thuộc tính mô tả gì thêm ngoài chính nó -> giữ thẳng trong fact
    s.order_id,
    s.product_id,

    -- Surrogate keys (khoá join tới các dimension)
    (TO_CHAR(s.order_date, 'YYYYMMDD'))::INT   AS date_id,
    c.customer_sk,
    p.product_sk,
    e.employee_sk,   -- có thể NULL nếu order_date nằm ngoài mọi khoảng hiệu lực của nhân viên (dữ liệu bất thường, xem CHECKPOINT)

    -- Degenerate attributes giữ lại để filter nhanh không cần join dim
    s.channel,
    s.region,
    s.province,
    s.delivery_status,
    s.payment_method,
    s.payment_status,

    -- Measures (số đo)
    s.quantity,
    s.unit_price,
    s.discount_pct,
    s.discount_amount,
    s.gross_amount,
    s.net_amount,

    NOW() AS _loaded_at

FROM schema_silver.sales_transactions s
LEFT JOIN schema_gold.dim_customers c
    ON s.customer_id = c.customer_id
LEFT JOIN schema_gold.dim_products p
    ON s.product_id = p.product_id
LEFT JOIN schema_gold.dim_employees e
    ON s.employee_id = e.employee_id
   AND s.order_date >= e.effective_date
   AND (s.order_date <= e.expiry_date OR e.expiry_date IS NULL);

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.fact_sales; -- kỳ vọng khớp số dòng stg_sales_transactions
-- SELECT COUNT(*) FROM schema_gold.fact_sales WHERE customer_sk IS NULL; -- kỳ vọng = 0 (mọi customer_id đều có trong dim_customers)
-- SELECT COUNT(*) FROM schema_gold.fact_sales WHERE product_sk IS NULL;  -- kỳ vọng = 0
-- SELECT COUNT(*) FROM schema_gold.fact_sales WHERE employee_sk IS NULL; -- nếu > 0, kiểm tra employee_id đó có tồn tại/có khoảng hiệu lực phủ đúng order_date không
-- SELECT SUM(net_amount) FROM schema_gold.fact_sales; -- tổng doanh thu, đối chiếu nhanh với số liệu bạn biết trước (nếu có)
