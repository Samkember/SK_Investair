import os, sys
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime
from datetime import timezone, timedelta
from dateutil.relativedelta import relativedelta

# === CONFIG ===
BUCKET = "xtf-asx"
RUN_DATE = datetime.now(timezone(timedelta(hours=10))).date() - relativedelta(days=1)
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
    s3 = boto3.client('s3')
    engine = get_mysql_engine()

    prefix = RUN_DATE.strftime("%Y%m%d") + "/"

    information = []

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

                    record = parse_txt_header(fields, file_id)
                    information.append(record)

                except Exception as fe:
                    print(f"❌ Failed to process file: {file_id} — {fe}")

    except Exception as e:
        print(f"❌ Error processing folder {prefix}: {e}")
        break


    df = pd.DataFrame(information)
    insert_into_sql(df, engine)
    print(f"✅ Inserted {len(information)} records")


# === Run ===
if __name__ == "__main__":
    process_folders()

# def lambda_handler(event=None, context=None):
#     process_folders()
#     return {
#         "statusCode": 200,
#         "body": "ASX Annoucement Classification has been successfully run"
#     }
