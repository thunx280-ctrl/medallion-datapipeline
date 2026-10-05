import sys
import os

# Thêm thư mục gốc vào path để import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from sqlalchemy import text

from utils.db_utils import get_db_engine

# Task 2.1: Data Profiling cho toàn bộ Bronze layer
#
# Với mỗi bảng bên dưới, script sẽ:
#   1. Đọc toàn bộ dữ liệu từ schema_bronze.<table>
#   2. Với TỪNG CỘT, tính: dtype, số dòng, số ô null/rỗng, % null,
#      số giá trị duy nhất, 3 giá trị mẫu
#   3. Ghi kết quả vào bảng schema_bronze.profiling_summary (xoá kết quả
#      cũ của bảng đó trước khi ghi mới, để không bị trùng lặp qua nhiều lần chạy)
#   4. Xuất TẤT CẢ kết quả ra 1 file Excel (mỗi bảng = 1 sheet riêng),
#      để dễ xem tổng quan mà không cần mở DB.


# Liệt kê đúng 10 bảng Bronze đã load ở Phase 1 (khớp TABLE_NAME trong loader/*.py)
BRONZE_TABLES = [
    "customers",
    "products",
    "distributors",
    "distributor_orders",
    "employees",
    "territory_mapping",
    "return_transactions",
    "promotion_program",
    "sales_transactions",
    "sales_targets_raw",
]

# Các cột metadata do add_metadata() tự thêm — không cần profiling chi tiết
# (vì bản chất luôn có giá trị, không phản ánh chất lượng dữ liệu NGUỒN)
METADATA_COLUMNS = {"_source_file", "_source_platform", "_ingested_at", "_batch_id", "_sheet_name"}


def profile_table(engine, table_name, schema="schema_bronze"):
    """
    Profiling 1 bảng: trả về DataFrame kết quả (1 dòng = 1 cột của bảng gốc).
    """
    print(f"🔎 Đang profiling: {schema}.{table_name} ...")

    try:
        df = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", engine)
    except Exception as e:
        print(f"  ⚠️  Bỏ qua {table_name} (chưa có dữ liệu hoặc chưa tồn tại): {e}")
        return None

    total_rows = len(df)
    rows = []

    for col in df.columns:
        series = df[col]

        # Dữ liệu Bronze đa số là dtype=str (do read_data ép dtype=str,
        # keep_default_na=False), nên ô "rỗng" thường là chuỗi "" chứ không
        # phải NaN thật. Đếm cả 2 trường hợp cho chắc (cột _ingested_at là
        # datetime thật nên .isna() vẫn đúng).
        is_null = series.isna() | (series.astype(str).str.strip() == "")
        null_count = int(is_null.sum())
        null_pct = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0.0

        distinct_count = int(series[~is_null].nunique())

        # sample_values = (
        #     series[~is_null].astype(str).drop_duplicates().head(3).tolist()
        # )
        # sample_str = " | ".join(sample_values)

        rows.append({
            "table_name": table_name,
            "column_name": col,
            "dtype": str(series.dtype),
            "total_rows": total_rows,
            "null_count": null_count,
            "null_pct": null_pct,
            "distinct_count": distinct_count,
            # "sample_values": sample_str,
            "is_metadata_column": col in METADATA_COLUMNS,
        })

    result_df = pd.DataFrame(rows)
    print(f"  ✅ {table_name}: {total_rows} dòng, {len(df.columns)} cột")
    return result_df


def save_to_db(engine, result_df, table_name):
    """Xoá kết quả cũ của table_name, rồi ghi kết quả mới vào profiling_summary."""
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM schema_bronze.profiling_summary WHERE table_name = :t"),
            {"t": table_name},
        )

    # Bỏ cột is_metadata_column trước khi ghi (không có trong DDL profiling_summary)
    db_cols = ["table_name", "column_name", "dtype", "total_rows",
               "null_count", "null_pct", "distinct_count"]
    result_df[db_cols].to_sql(
        "profiling_summary",
        con=engine,
        schema="schema_bronze",
        if_exists="append",
        index=False,
    )


def run():
    engine = get_db_engine()
    all_results = {}

    for table_name in BRONZE_TABLES:
        result_df = profile_table(engine, table_name)
        if result_df is None:
            continue

        save_to_db(engine, result_df, table_name)
        all_results[table_name] = result_df

    if not all_results:
        print("❌ Không profiling được bảng nào — kiểm tra lại kết nối DB / dữ liệu Bronze.")
        return

    # Xuất Excel: 1 sheet "Summary" tổng hợp + 1 sheet riêng cho mỗi bảng
    output_path = os.path.join(
        os.path.dirname(__file__), "..", "Profiling_Report.xlsx"
    )
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_rows = []
        for table_name, df in all_results.items():
            summary_rows.append({
                "table_name": table_name,
                "total_rows": df["total_rows"].iloc[0],
                "total_columns": len(df),
                "columns_with_nulls": int((df["null_count"] > 0).sum()),
                "max_null_pct": df["null_pct"].max(),
            })
            # Tên sheet Excel tối đa 31 ký tự
            sheet_name = table_name[:31]
            df.drop(columns=["is_metadata_column"]).to_excel(writer, sheet_name=sheet_name, index=False)

        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

    print(f"\n📊 Đã xuất báo cáo: {os.path.abspath(output_path)}")
    print(f"📥 Đã ghi kết quả vào bảng schema_bronze.profiling_summary ({len(all_results)} bảng)")


if __name__ == "__main__":
    run()
