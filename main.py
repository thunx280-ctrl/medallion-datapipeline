import os
import sys
import time
import uuid
import importlib
import traceback

import psycopg2
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)
sys.path.append(os.path.join(BASE_DIR, "loader"))

load_dotenv()

# ============================================================================
# main.py — Orchestrator: chạy toàn bộ pipeline Bronze -> Silver -> Gold
#
# NGUYÊN TẮC FAIL-FAST GIỮA CÁC STAGE: nếu Silver lỗi, KHÔNG chạy Gold
# (vì Gold phụ thuộc trực tiếp vào Silver). Nếu Bronze lỗi ở 1-2 nguồn,
# vẫn cho phép chạy tiếp Silver/Gold cho các nguồn KHÔNG lỗi (xem ghi chú
# ở run_bronze() bên dưới) — đây là lựa chọn thiết kế có chủ đích, không
# phải thiếu sót.
# ============================================================================

# Thứ tự BẮT BUỘC — đổi thứ tự có thể gây lỗi "relation does not exist"
LOADER_MODULES = [
    "load_customer",
    "load_product",
    "load_distributor",
    "load_distributor_orders",
    "load_employee",
    "loader_territory",
    "load_return",
    "load_promotion",
    "load_sales_transaction",
    "load_sales_target",
]

SILVER_FILES = [
    "silver/silver_customers.sql",
    "silver/silver_products.sql",
    "silver/silver_distributors.sql",
    "silver/silver_distributor_orders.sql",
    "silver/silver_employees.sql",
    "silver/silver_territoty.sql",
    "silver/silver_return_transactions.sql",
    "silver/silver_promotion_program.sql",
    "silver/silver_sales_transactions.sql",
    "silver/silver_sales_targets.sql",
]

# Dim trước, fact sau (cần dim), mart cuối cùng (cần fact)
GOLD_FILES = [
    "gold/dim_date.sql",
    "gold/dim_customers.sql",
    "gold/dim_products.sql",
    "gold/dim_employees.sql",
    "gold/dim_distributors.sql",
    "gold/fact_sales.sql",
    "gold/fact_targets.sql",
    "gold/fact_returns.sql",
    "gold/mart_sales_vs_target.sql",
    "gold/mart_distributor_performance.sql",
]


def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def run_sql_file(conn, filepath):
    """Đọc và chạy toàn bộ nội dung 1 file .sql (có thể nhiều câu lệnh)."""
    with open(filepath, "r", encoding="utf-8") as f:
        sql_text = f.read()
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()


def run_sql_batch(conn, relative_paths):
    for rel_path in relative_paths:
        full_path = os.path.join(BASE_DIR, rel_path)
        print(f"  → {rel_path}")
        run_sql_file(conn, full_path)


def run_bronze():
    """
    Chạy lần lượt 10 loader. LƯU Ý: mỗi loader/*.py đã tự bẫy lỗi và ghi
    log riêng vào schema_bronze.ingest_log — nếu 1 loader lỗi, nó IN RA
    LỖI nhưng KHÔNG raise exception, nên run_bronze() này sẽ KHÔNG dừng
    giữa chừng dù có nguồn bị lỗi. Đây là lựa chọn có chủ đích: 1 nguồn
    lỗi (vd sales_transactions bị lỗi mạng khi tải) không nên chặn 9
    nguồn còn lại load thành công.
    -> Muốn biết nguồn nào lỗi, xem schema_bronze.ingest_log sau khi
       chạy xong, KHÔNG chỉ nhìn vào schema_gold.pipeline_logs (bảng đó
       chỉ ghi 1 dòng SUCCESS cho toàn bộ stage BRONZE).
    """
    for module_name in LOADER_MODULES:
        module = importlib.import_module(module_name)
        module.run()


def log_pipeline(conn, batch_id, stage, status, message, duration_sec):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO schema_gold.pipeline_logs
                (batch_id, pipeline_stage, status, message, duration_sec, finished_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (str(batch_id), stage, status, message, round(duration_sec, 2)),
        )
    conn.commit()


def run_stage(conn, batch_id, stage_name, action):
    """Chạy 1 stage, đo thời gian, bẫy lỗi, ghi log vào pipeline_logs."""
    print(f"\n{'=' * 15} {stage_name} {'=' * 15}")
    start = time.time()
    try:
        action()
        duration = time.time() - start
        log_pipeline(conn, batch_id, stage_name, "SUCCESS", None, duration)
        print(f"✅ {stage_name} thành công ({round(duration, 2)}s)")
        return True
    except Exception as e:
        duration = time.time() - start
        print(f"❌ {stage_name} THẤT BẠI: {e}")
        traceback.print_exc()
        log_pipeline(conn, batch_id, stage_name, "FAILED", str(e), duration)
        return False


def main():
    batch_id = uuid.uuid4()
    conn = get_pg_connection()

    print(f"🚀 Bắt đầu chạy pipeline VietDist — batch_id: {batch_id}")

    if not run_stage(conn, batch_id, "BRONZE", run_bronze):
        # Thực tế run_bronze() gần như luôn trả về SUCCESS (xem docstring
        # run_bronze) — nhánh này chỉ kích hoạt nếu có lỗi HẠ TẦNG (vd mất
        # kết nối DB hoàn toàn), không phải lỗi 1 nguồn đơn lẻ.
        print("⛔ Dừng pipeline: lỗi hạ tầng ở stage BRONZE.")
        conn.close()
        return

    if not run_stage(conn, batch_id, "SILVER", lambda: run_sql_batch(conn, SILVER_FILES)):
        print("⛔ Dừng pipeline: Silver lỗi -> không chạy Gold (Gold phụ thuộc Silver).")
        conn.close()
        return

    gold_ok = run_stage(conn, batch_id, "GOLD", lambda: run_sql_batch(conn, GOLD_FILES))

    conn.close()

    if gold_ok:
        print(f"\n🎉 Pipeline hoàn tất thành công! batch_id: {batch_id}")
    else:
        print(f"\n⚠️  Pipeline kết thúc với lỗi ở GOLD. batch_id: {batch_id}")
        print("   Kiểm tra schema_gold.pipeline_logs để xem chi tiết lỗi.")


if __name__ == "__main__":
    main()
