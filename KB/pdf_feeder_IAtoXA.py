import boto3
import json
import logging
from itertools import islice

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

# --- AWS Clients ---
s3 = boto3.client("s3")
sqs = boto3.client("sqs")

# --- Constants ---
SRC_BUCKET = "investair-asx"
DEST_BUCKET = "xtf-asx"
SQS_URL = "https://sqs.ap-southeast-2.amazonaws.com/025916830954/xtf_asx_restorer_queue"
SQS_BATCH_SIZE = 10  # AWS limit

def list_pdf_keys(bucket_name):
    """Return dict of filename → full S3 key for all .pdf files."""
    paginator = s3.get_paginator("list_objects_v2")
    key_map = {}
    total = 0

    logger.info(f"🔍 Scanning for PDFs in: {bucket_name}")
    try:
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                total += 1
                key = obj["Key"]
                if total % 1000 == 0:
                    logger.info(f"🧮 Scanned {total} objects in {bucket_name}...")

                if key.lower().endswith(".pdf") and key.count("/") >= 4:
                    filename = key.split("/")[-1].lower()
                    key_map[filename] = key
        logger.info(f"✅ Found {len(key_map)} PDF files in {bucket_name} (out of {total} objects)")
        return key_map
    except Exception as e:
        logger.error(f"❌ Failed to list objects from {bucket_name}: {e}")
        return {}

def chunked(iterable, size):
    """Yield successive size-sized chunks from iterable."""
    iterator = iter(iterable)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            break
        yield batch

def queue_missing_files():
    logger.info("📥 Getting file lists from both buckets...")

    src_files = list_pdf_keys(SRC_BUCKET)
    dest_files = list_pdf_keys(DEST_BUCKET)
    dest_filenames = set(dest_files.keys())

    logger.info(f"📊 Source PDFs: {len(src_files)} | Destination PDFs: {len(dest_filenames)}")

    # Determine which files are missing in destination
    missing_files = {
        filename: src_key for filename, src_key in src_files.items()
        if filename not in dest_filenames
    }

    logger.info(f"🧮 Total missing files to queue: {len(missing_files)}")
    total_queued = 0

    items = list(missing_files.items())
    for batch_idx, batch in enumerate(chunked(items, SQS_BATCH_SIZE)):
        entries = [
            {
                "Id": f"msg{i}",
                "MessageBody": json.dumps({
                    "filename": filename,
                    "source_key": source_key
                })
            }
            for i, (filename, source_key) in enumerate(batch)
        ]

        try:
            response = sqs.send_message_batch(QueueUrl=SQS_URL, Entries=entries)
            success = response.get("Successful", [])
            failed = response.get("Failed", [])

            total_queued += len(success)
            logger.info(f"📤 Batch {batch_idx+1}: Sent {len(success)} | Failed {len(failed)}")

            if failed:
                for f in failed:
                    logger.warning(f"⚠️ Failed message ID: {f.get('Id')} — {f.get('Message')}")
        except Exception as e:
            logger.error(f"❌ Exception sending batch {batch_idx+1}: {e}")

    logger.info(f"✅ Done queuing. Total messages queued: {total_queued}")
    return total_queued

def lambda_handler(event, context):
    logger.info("🚀 Lambda S3-to-SQS scanner started")
    total = queue_missing_files()
    logger.info(f"🎯 Lambda completed. Total SQS messages queued: {total}")
    return {
        "statusCode": 200,
        "body": f"Queued {total} missing PDF files to SQS"
    }
