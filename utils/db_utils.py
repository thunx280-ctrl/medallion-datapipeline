import os
from io import BytesIO
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from datetime import datetime
import uuid


load_dotenv()

SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SERVICE_ACCOUNT_KEY')
FOLDER_DRIVE_ID = os.getenv('GOOGLE_DRIVE_FOLDER_ID')

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def get_service_account():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes = SCOPES
    )
    service = build(
        "drive",
        "v3",
        credentials= credentials
    )
    return service

def download_data(service, file_id):
    request = service.files().get_media(
        fileId= file_id,
        supportsAllDrives = True
    )

    buffer = BytesIO()
    dowloader = MediaIoBaseDownload(buffer, request)
    done =False
    while not done:
        status, done = dowloader.next_chunk()

    buffer.seek(0)
    return buffer

def get_db_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(url)


def new_batch_id():
    """
    dùng để sinh batch_id DUY NHẤT 1 LẦN ở đầu hàm run() của mỗi
    loader, rồi truyền xuống cho add_metadata() của MỌI file trong lần chạy đó.
    """
    return str(uuid.uuid4())

def add_metadata(df, source_file, source_platform="google_drive", batch_id=None):
    if batch_id is None:
        batch_id = str(uuid.uuid4())
    df = df.copy()
    df['_source_file'] = source_file
    df['_source_platform'] = source_platform
    df['_ingested_at'] = datetime.now()
    df['_batch_id'] = batch_id
    return df

def load_to_bronze(df, table_name, engine, schema='schema_bronze', if_exists='append'):

    # Nguyên tắc Bronze: Ép tất cả các cột về kiểu string/text để tránh lỗi ép kiểu
    # df_bronze = df.astype(str)

    # Ghi dữ liệu vào Postgres
    rows_affected = df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False
    )
    return len(df)

def log_ingestion(
        engine,
        batch_id,
        source_name,
        source_file,
        source_platform,
        rows_loaded,
        status,
        duration_sec,
        error_message=None):
    # Ghi vết lịch sử nạp dữ liệu vào bảng raw.ingest_log.
    query = text("""
        INSERT INTO schema_bronze.ingest_log (batch_id, source_name, source_file, source_platform, rows_loaded, status, duration_sec, error_message)
        VALUES (:batch_id, :source_name, :source_file, :source_platform, :rows_loaded, :status, :duration_sec, :error_message)
    """)
    with engine.begin() as conn:
        conn.execute(query, {
            "batch_id": batch_id,
            "source_name": source_name,
            "source_file": source_file,
            "source_platform": source_platform,
            "rows_loaded": rows_loaded,
            "status": status,
            "duration_sec": duration_sec,
            "error_message": error_message
        })
if __name__ == "__main__":
    # Kiểm tra thử kết nối
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            print("[SUCCESS] Kết nối đến PostgreSQL thành công!")
    except Exception as e:
        print(f"[ERROR] Kết nối thất bại: {e}")

    # # Kiểm tra thử kết nối Drive + liệt kê file trong folder chung
    # try:
    #     service = get_service_account()
    #     files = list_files_in_drive(service)
    #     print(f"[SUCCESS] Tìm thấy {len(files)} file trong folder Drive:")
    #     for f in files:
    #         print(f"  - {f['name']} ({f['id']})")
    # except Exception as e:
    #     print(f"[ERROR] Kết nối Drive thất bại: {e}")
