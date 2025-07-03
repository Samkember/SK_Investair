import os
import json
import boto3
import time
from botocore.exceptions import ClientError
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from datetime import datetime
import pandas as pd

# --- Configuration ---
SRC_BUCKET = "xtf-asx"
QUEUE_URL = "https://sqs.ap-southeast-2.amazonaws.com/025916830954/annoucement_txt_queue"
DB_HOST = os.environ["DB_HOST"]
DB_USER = os.environ["DB_USER"]
DB_PW = os.environ["DB_PASSWORD"]
DB_PORT = int(os.environ.get("DB_PORT", 3306))
DB_NAME = "ASX_Market"
TABLE_NAME = "ASX_Announcement_Metadata"


# --- AWS Clients ---
s3 = boto3.client("s3")
sqs = boto3.client("sqs")

# --- SQL Engine ---
MYSQL_URI = f"mysql+pymysql://{DB_USER}:{DB_PW}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        print("🔧 Creating DB engine (NullPool)...")
        _engine = create_engine(MYSQL_URI, pool_pre_ping=True, poolclass=NullPool)
    return _engine

# --- Helpers ---
def list_txt_files_in_s3(bucket):
    print(f"🔍 Listing .txt files in bucket: {bucket}")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".txt"):
                keys.append(key)
    print(f"📄 Found {len(keys)} .txt files")
    return keys

def get_already_processed_files():
    print("🗃️ Fetching processed filenames from DB...")
    t0 = time.time()
    try:
        sql = f"SELECT filename FROM {TABLE_NAME}"
        engine = get_engine()
        df = pd.read_sql(sql, engine)
        engine.dispose()  # 🧼 Clean up DB connection
        processed = set(df["filename"].str.strip().str.lower())
        print(f"✅ Retrieved {len(processed)} processed in {time.time() - t0:.2f}s")
        return processed
    except Exception as e:
        print(f"❌ DB connection or query failed: {e}")
        return set()

def format_sqs_messages(keys):
    for key in keys:
        base = os.path.basename(key).replace(".txt", "")
        yield {
            "Id": base[:80],
            "MessageBody": json.dumps({
                "Type": "Notification",
                "Message": json.dumps({
                    "Records": [{
                        "s3": {"object": {"key": key}}
                    }]
                }),
                "Timestamp": datetime.utcnow().isoformat()
            })
        }

# --- Lambda Entrypoint ---
def lambda_handler(event, context):
    print("🚀 Lambda execution started")
    print(f"📦 Event payload: {json.dumps(event)}")

    # ❌ Skip execution if triggered from EventBridge (e.g., scheduled trigger)
    if event.get("source") == "aws.events":
        print("⏭️ Triggered by EventBridge — skipping execution.")
        return {"statusCode": 200, "body": "Skipped execution — EventBridge trigger detected."}

    try:
        all_txt_keys = list_txt_files_in_s3(SRC_BUCKET)
        processed_files = get_already_processed_files()

        new_txt_keys = [
            k for k in all_txt_keys
            if os.path.basename(k).replace(".txt", "").lower() not in processed_files
        ]

        print(f"📥 {len(new_txt_keys)} total unprocessed files found.")

        if not new_txt_keys:
            print("✅ No new .txt files to process.")
            return {"statusCode": 200, "body": "No new files found."}

        # Use new_txt_keys instead of limited_keys
        for i in range(0, len(new_txt_keys), 10):  # Process the unprocessed files in batches of 10
            batch = new_txt_keys[i:i + 10]
            entries = list(format_sqs_messages(batch))
            print(f"📤 Sending SQS batch: {[e['Id'] for e in entries]}")

            try:
                response = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
                print(f"✅ SQS: {len(response.get('Successful', []))} success, {len(response.get('Failed', []))} fail")
                if response.get("Failed"):
                    print(f"⚠️ Failed entries: {response['Failed']}")
            except ClientError as e:
                print(f"❌ Error sending batch: {e}")
    except Exception as e:
        print(f"🔥 FATAL ERROR: {e}", flush=True)
        raise

    print("🏁 Lambda execution complete.")
    return {"statusCode": 200, "body": f"Enqueued {len(new_txt_keys)} files"}
