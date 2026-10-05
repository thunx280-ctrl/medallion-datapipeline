import sys
import os
import time

# Thêm thư mục gốc vào path để import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.db_utils import get_service_account, download_data, get_db_engine, add_metadata, load_to_bronze, log_ingestion, new_batch_id
from utils.file_parser import read_data

FILE_ID = os.getenv("FILE_ID_SALES_TRANSACTIONS")
FILE_NAME = "sales_transactions.csv"
TABLE_NAME = "sales_transactions"
SOURCE_NAME = "sales_transactions"

def run():
    batch_id = new_batch_id()
    engine = get_db_engine()
    service = get_service_account()

    start_time = time.time()
    print(f"🚀 Bắt đầu nạp file: {FILE_NAME}")

    try:
        # 1. Read Data
        buffer = download_data(service, FILE_ID)
        df = read_data(FILE_NAME, buffer)

        # 2. Add Metadata
        df = add_metadata(df, source_file=FILE_NAME, source_platform="google_drive", batch_id=batch_id)

        # 3. Load to Bronze Layer
        rows = load_to_bronze(df, TABLE_NAME, engine)

        duration = round(time.time() - start_time, 2)

        # 4. Log Success
        log_ingestion(
            engine=engine, batch_id=batch_id, source_name=SOURCE_NAME,
            source_file=FILE_NAME, source_platform="google_drive",
            rows_loaded=rows, status="SUCCESS", duration_sec=time.time() - start_time,
        )
        print(f"✅ Nạp thành công {rows} dòng vào schema_bronze.{TABLE_NAME} ({duration}s)")

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
