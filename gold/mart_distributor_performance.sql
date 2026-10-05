-- =====================================================================
-- mart_distributor_performance.sql
-- Gold layer: schema_silver.distributor_orders + dim_distributors
--            -> schema_gold.mart_distributor_performance
-- Mục đích: tổng hợp hiệu suất từng nhà phân phối theo tháng (số đơn,
-- tỷ lệ giao đủ hàng - fill rate, tỷ lệ giao đúng hẹn - on-time rate).

DROP TABLE IF EXISTS schema_gold.mart_distributor_performance;

CREATE TABLE schema_gold.mart_distributor_performance AS
SELECT
    dd.distributor_sk,
    dd.distributor_id,
    dd.distributor_name,
    dd.tier,
    dd.region,
    d.year,
    d.month,

    COUNT(DISTINCT o.order_id)                       AS total_orders,
    SUM(o.qty_ordered)                                AS total_qty_ordered,
    SUM(o.qty_delivered)                              AS total_qty_delivered,
    SUM(o.gross_amount)                               AS total_gross_amount,
    SUM(o.delivered_amount)                           AS total_delivered_amount,
    ROUND(AVG(o.fill_rate_pct), 2)                    AS avg_fill_rate_pct,

    -- GIẢ ĐỊNH cần xác nhận (xem ghi chú ở đầu file)
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE UPPER(o.ontime_delivery) IN ('YES', 'TRUE', '1', 'Y'))
        / NULLIF(COUNT(*), 0)
    , 2)                                                AS ontime_delivery_rate_pct,

    NOW() AS _loaded_at

FROM schema_silver.distributor_orders o
JOIN schema_gold.dim_date d
    ON (TO_CHAR(o.order_date, 'YYYYMMDD'))::INT = d.date_id
LEFT JOIN schema_gold.dim_distributors dd
    ON o.distributor_id = dd.distributor_id
   AND o.order_date >= dd.effective_date
   AND (o.order_date <= dd.expiry_date OR dd.expiry_date IS NULL)
GROUP BY dd.distributor_sk, dd.distributor_id, dd.distributor_name, dd.tier, dd.region, d.year, d.month;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.mart_distributor_performance;
-- SELECT COUNT(*) FROM schema_gold.mart_distributor_performance WHERE distributor_sk IS NULL; -- kỳ vọng = 0
-- SELECT distributor_name, AVG(avg_fill_rate_pct) FROM schema_gold.mart_distributor_performance
--   GROUP BY distributor_name ORDER BY 2 ASC LIMIT 10; -- 10 nhà phân phối có fill rate thấp nhất, đáng chú ý
