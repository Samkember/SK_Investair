import os
import json
import time
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# --- Configuration ---
SRC_BUCKET = "xtf-asx"
DEST_BUCKET = "investair-asx"
QUEUE_URL = "https://sqs.ap-southeast-2.amazonaws.com/025916830954/annoucement_txt_queue"

# --- AWS Clients ---
s3 = boto3.client("s3")
sqs = boto3.client("sqs")

# --- Helpers ---
def list_pdf_files(bucket):
    print(f"🔍 Listing .pdf files in S3 bucket: {bucket}")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".pdf"):
                keys.append(key)
    print(f"📄 Found {len(keys)} .pdf files in source bucket")
    return keys

def list_existing_pdf_basenames(bucket):
    print(f"🔍 Listing existing .pdf files in destination bucket: {bucket}")
    paginator = s3.get_paginator("list_objects_v2")
    basenames = set()
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".pdf"):
                base = os.path.basename(key).replace(".pdf", "").lower()
                basenames.add(base)
    print(f"📄 Found {len(basenames)} existing .pdf base names")
    return basenames

def format_sqs_messages(keys):
    for key in keys:
        base = os.path.basename(key).replace(".pdf", "")
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

    if event.get("source") == "aws.events":
        print("⏭️ Triggered by EventBridge — skipping execution.")
        return {"statusCode": 200, "body": "Skipped execution — EventBridge trigger detected."}

    try:
        all_pdf_keys = list_pdf_files(SRC_BUCKET)
        existing_pdf_basenames = list_existing_pdf_basenames(DEST_BUCKET)

        # Filter only PDFs not present in destination
        unprocessed_pdf_keys = [
            k for k in all_pdf_keys
            if os.path.basename(k).replace(".pdf", "").lower() not in existing_pdf_basenames
        ]

        print(f"📥 {len(unprocessed_pdf_keys)} new .pdf files to queue")

        if not unprocessed_pdf_keys:
            print("✅ No new .pdf files to process.")
            return {"statusCode": 200, "body": "No new files to queue."}

        for i in range(0, len(unprocessed_pdf_keys), 10):
            batch = unprocessed_pdf_keys[i:i + 10]
            entries = list(format_sqs_messages(batch))

            for entry in entries:
                try:
                    msg = json.loads(entry["MessageBody"])
                    record = json.loads(msg["Message"])["Records"][0]
                    file_key = record["s3"]["object"]["key"]
                    print(f"📨 Enqueuing file: {file_key}")
                except Exception as e:
                    print(f"⚠️ Error extracting filename for log: {e}")

            print(f"📤 Sending SQS batch: {[e['Id'] for e in entries]}")

            try:
                response = sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
                success = len(response.get("Successful", []))
                failed = len(response.get("Failed", []))
                print(f"✅ SQS: {success} success, {failed} fail")
                if failed:
                    print(f"⚠️ Failed entries: {response['Failed']}")
            except ClientError as e:
                print(f"❌ Error sending batch: {e}", flush=True)

    except Exception as e:
        print(f"🔥 FATAL ERROR: {e}", flush=True)
        raise

    print("🏁 Lambda execution complete.")
    return {"statusCode": 200, "body": f"Enqueued {len(unprocessed_pdf_keys)} PDFs"}
