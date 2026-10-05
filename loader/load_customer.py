import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.db_utils import (
    get_service_account, download_data, get_db_engine,
    add_metadata, load_to_bronze, log_ingestion, new_batch_id,
)
from utils.file_parser import read_data

FILE_ID = os.getenv("FILE_ID_CUSTOMERS")
FILE_NAME = "customer_master.csv"   # chỉ dùng để suy ra định dạng (.csv/.xlsx...) và ghi log
TABLE_NAME = "customers"
SOURCE_NAME = "customer_master"

def run():
    batch_id = new_batch_id()
    engine = get_db_engine()
    service = get_service_account()

    start_time = time.time()
    try:
        buffer = download_data(service, file_id=FILE_ID)
        df = read_data(FILE_NAME, buffer)
        df = add_metadata(df, source_file=FILE_NAME, source_platform="google_drive", batch_id=batch_id)
        rows = load_to_bronze(df, TABLE_NAME, engine)

        log_ingestion(
            engine=engine, batch_id=batch_id, source_name=SOURCE_NAME,
            source_file=FILE_NAME, source_platform="google_drive",
            rows_loaded=rows, status="SUCCESS", duration_sec=time.time() - start_time,
        )
        print(f"[SUCCESS] {SOURCE_NAME}: đã load {rows} dòng vào schema_bronze.{TABLE_NAME}")

    except Exception as e:
        log_ingestion(
            engine=engine, batch_id=batch_id, source_name=SOURCE_NAME,
            source_file=FILE_NAME, source_platform="google_drive",
            rows_loaded=0, status="FAILED", duration_sec=time.time() - start_time,
            error_message=str(e),
        )
        print(f"[ERROR] {SOURCE_NAME}: {e}")


if __name__ == "__main__":
    run()
