import sys
import os
import time
from datetime import datetime

# Thêm thư mục gốc vào path để import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import text

from utils.db_utils import get_service_account, download_data, get_db_engine, log_ingestion, new_batch_id
from utils.file_parser import read_data

FILE_ID = os.getenv("FILE_ID_SALES_TARGET")
FILE_NAME = "sales_target_plan.xlsx"
SOURCE_NAME = "sales_target_plan"

# Đúng theo cấu trúc bảng schema_bronze.sales_targets_raw trong init_db.sql
TARGET_COLUMNS = [
    "plan_version", "version_date", "effective_from", "effective_to",
    "employee_id", "employee_name", "region", "team", "year", "month",
    "target_revenue", "target_quantity", "target_new_customers",
]


def _extract_version_label(sheet_name, df):
    if "plan_version" in df.columns and df["plan_version"].astype(bool).any():
        return str(df["plan_version"].dropna().iloc[0])
    for part in sheet_name.split("_"):
        if part.lower().startswith("v") and part[1:].isdigit():
            return part
    return sheet_name


def _clean_sheet(df):
    # Chuẩn hoá tên cột về chữ thường, bỏ khoảng trắng thừa
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Loại bỏ dòng "Tổng"/"Total" nếu có
    if "month" in df.columns:
        mask_total = df["month"].astype(str).str.strip().str.lower().isin(
            ["tổng", "tong", "total"]
        )
        df = df[~mask_total]

    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[TARGET_COLUMNS]


def run():
    batch_id = new_batch_id()
    engine = get_db_engine()
    service = get_service_account()

    start_time = time.time()
    print(f"🚀 Bắt đầu nạp file: {FILE_NAME} (nhiều sheet/version)")

    try:
        # 1. Read Data — sheet_name=None để lấy TẤT CẢ sheet (mỗi sheet = 1 version)
        buffer = download_data(service, FILE_ID)
        all_sheets = read_data(FILE_NAME, buffer, sheet_name=None)

        total_rows = 0
        for sheet_name, df in all_sheets.items():
            if df.empty:
                continue

            df_clean = _clean_sheet(df)
            version_label = _extract_version_label(sheet_name, df_clean)

            # 2. Add Metadata (thủ công vì bảng này có cột riêng, không dùng add_metadata() chung)
            df_clean["_source_file"] = FILE_NAME
            df_clean["_sheet_name"] = sheet_name
            df_clean["_source_platform"] = "google_drive"
            df_clean["_ingested_at"] = datetime.now()
            df_clean["_batch_id"] = batch_id


            # 3. Load to Bronze Layer — append, KHÔNG ghi đè version cũ
            df_clean.to_sql(
                "sales_targets_raw",
                con=engine,
                schema="schema_bronze",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )

            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO schema_bronze.sales_target_files
                            (source_file, sheet_name, version_label, row_count, _batch_id)
                        VALUES (:source_file, :sheet_name, :version_label, :row_count, :batch_id)
                        """
                    ),
                    dict(
                        source_file=FILE_NAME,
                        sheet_name=sheet_name,
                        version_label=version_label,
                        row_count=len(df_clean),
                        batch_id=batch_id,
                    ),
                )

            total_rows += len(df_clean)
            print(f"  ↳ sheet '{sheet_name}' ({version_label}) — {len(df_clean)} dòng")

        duration = round(time.time() - start_time, 2)

        # 4. Log Success
        log_ingestion(
            engine=engine, batch_id=batch_id, source_name=SOURCE_NAME,
            source_file=FILE_NAME, source_platform="google_drive",
            rows_loaded=total_rows, status="SUCCESS", duration_sec=time.time() - start_time,
        )
        print(f"✅ Nạp thành công {total_rows} dòng, {len(all_sheets)} sheet(s) ({duration}s)")

    except Exception as e:
        log_ingestion(
            engine=engine, batch_id=batch_id, source_name=SOURCE_NAME,
            source_file=FILE_NAME, source_platform="google_drive",
            rows_loaded=0, status="FAILED", duration_sec=time.time() - start_time,
            error_message=str(e)
        )
        print(f"❌ Lỗi nạp file {FILE_NAME}: {e}")

if __name__ == "__main__":
    run()
