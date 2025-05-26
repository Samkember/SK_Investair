import os, sys
from s3_manager import S3Manager
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime

# === CONFIG ===
BUCKET = "xtf-asx"
START_DATE = "20240301"
END_DATE = "20250523"
outputFolderPath = r"C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms\ASX_SS_Forms_2\Output"
CHECKPOINT_FILE = os.path.join(outputFolderPath, "txt_file_classification_all_S3_History_Checkpoint.txt")
BATCH_SIZE = 10
TABLE_NAME = "ASX_Annoucement_HeaderFiles"
SCHEMA_NAME = "Substantial_Holding"

# === DATABASE CONNECTION ===
def get_mysql_engine(mysql_url=None):
    if mysql_url is None:
        mysql_url = (
            "mysql+pymysql://sam:sam2025@"
            "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/{schema_name}".format(schema_name=SCHEMA_NAME)
        )
    return create_engine(mysql_url)

# === TRACK PROGRESS ===
def read_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return f.read().strip()
    return START_DATE

def write_checkpoint(date_str):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(date_str)

# === SQL INSERT ===
def insert_into_sql(df, engine):
    with engine.begin() as conn:
        df.to_sql(TABLE_NAME, con=conn, if_exists='append', index=False, method='multi')

# === FILE EXISTS CHECK ===
def file_already_processed(conn, file_id):
    result = conn.execute(text(f"SELECT 1 FROM {TABLE_NAME} WHERE filename = :f"), {"f": file_id})
    return result.scalar() is not None

# === PARSE HEADER ===
def parse_txt_header(fields, file_id):
    rep_types = [f.strip() for f in fields[7:27] if f.strip()]
    return {
        "filename": file_id,
        "announcement_number": fields[32] if len(fields) > 32 else None,
        "asx_code": fields[33] if len(fields) > 33 else None,
        "exchange": fields[34] if len(fields) > 34 else None,
        "sensitive": fields[35] if len(fields) > 35 else None,
        "headline": fields[36] if len(fields) > 36 else None,
        "rec_type": fields[4] if len(fields) > 4 else None,
        "rec_date": fields[5] if len(fields) > 5 else None,
        "rec_time": fields[6] if len(fields) > 6 else None,
        "rel_date": fields[27] if len(fields) > 27 else None,
        "rel_time": fields[28] if len(fields) > 28 else None,
        "n_pages": int(fields[0]) if fields[0].isdigit() else None,
        "rep_types": str(rep_types)
    }

# === FORMAT CHECK ===
def is_valid_format(fields):
    try:
        if len(fields) < 37:
            return False
        int(fields[0])
        fields[4] and fields[5] and fields[6]
        fields[32] and fields[33]
        return True
    except Exception:
        return False

# === MAIN PROCESS ===
def process_folders():
    s3 = S3Manager()
    engine = get_mysql_engine()

    current_date = read_checkpoint()
    date_range = pd.date_range(start=current_date, end=END_DATE, freq='D')

    batch = []
    failed_files = []

    for date in date_range:
        prefix = date.strftime("%Y%m%d") + "/"
        print(f"\n📦 Processing folder: {prefix}")

        try:
            txt_files = s3.list_files(bucket_name=BUCKET, folder=prefix)
            txt_files = [f for f in txt_files if f.endswith(".txt")]

            with engine.begin() as conn:
                for file in txt_files:
                    raw_filename = os.path.basename(file).replace(".txt", "")
                    file_id = prefix[:8] + raw_filename  # e.g., '20240305' + '02781371'

                    if file_already_processed(conn, file_id):
                        continue

                    try:
                        raw_output = s3.get_file_bytes(bucket_name=BUCKET, key=file)
                        decoded = raw_output.decode('utf-8')
                        fields = decoded.splitlines()

                        if not is_valid_format(fields):
                            print(f"⚠️ Invalid format or malformed file: {file_id}")
                            failed_files.append(file_id)
                            continue

                        record = parse_txt_header(fields, file_id)
                        batch.append(record)

                        if len(batch) >= BATCH_SIZE:
                            df = pd.DataFrame(batch)
                            insert_into_sql(df, engine)
                            print(f"✅ Inserted batch of {len(batch)} records into DB")
                            batch.clear()
                    except Exception as fe:
                        print(f"❌ Failed to process file: {file_id} — {fe}")
                        failed_files.append(file_id)

            write_checkpoint(date.strftime("%Y%m%d"))

        except Exception as e:
            print(f"❌ Error processing folder {prefix}: {e}")
            break

    if batch:
        df = pd.DataFrame(batch)
        insert_into_sql(df, engine)
        print(f"✅ Final insert of {len(batch)} records")

    if failed_files:
        failed_df = pd.DataFrame({"failed_files": failed_files})
        failed_df.to_csv(os.path.join(outputFolderPath, "failed_files.csv"), index=False)
        print(f"⚠️ Saved {len(failed_files)} failed files to failed_files.csv")

if __name__ == "__main__":
    process_folders()