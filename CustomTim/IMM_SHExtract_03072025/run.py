import os
import pandas as pd
from sqlalchemy import create_engine, text
from s3_manager import S3Manager
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
DB_HOST = "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com"
DB_USER = "sam"
DB_PASSWORD = "sam2025"
DB_NAME = "ASX_Market"
TABLE_NAME = "ASX_Announcement_Metadata"
S3_BUCKET_NAME = "xtf-asx"
LOCAL_DOWNLOAD_DIR = "downloads"
MAX_WORKERS = 10  # Number of threads to use

# Ensure local download directory exists
os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

# --- Database connection ---
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
query = text(f"""
    SELECT filename
    FROM {TABLE_NAME}
    WHERE asx_code = 'IMM'
      AND (
        rep_types LIKE '%02001%' OR
        rep_types LIKE '%02002%' OR
        rep_types LIKE '%02003%'
      )
""")
with engine.connect() as conn:
    df = pd.read_sql(query, conn)

# --- Initialize S3Manager ---
s3 = S3Manager()
all_keys = s3.list_s3_objects(S3_BUCKET_NAME)
key_map = {os.path.basename(k): k for k in all_keys}

# --- Download task function ---
def download_task(base_filename):
    filename_with_ext = base_filename.strip() + ".pdf"

    if filename_with_ext in key_map:
        s3_key = key_map[filename_with_ext]
        local_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename_with_ext)

        try:
            print(f"⬇️  Downloading: {s3_key}")
            s3.download_file(
                object_name=s3_key,
                bucket_name=S3_BUCKET_NAME,
                file_name=local_path
            )
            return f"✅ Downloaded: {filename_with_ext}"
        except Exception as e:
            return f"❌ Failed: {filename_with_ext} — {e}"
    else:
        return f"❌ Not found in S3: {filename_with_ext}"

# --- Run downloads concurrently ---
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(download_task, row["filename"]) for _, row in df.iterrows()]
    for future in as_completed(futures):
        print(future.result())
