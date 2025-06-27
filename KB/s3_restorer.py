import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from s3_manager import S3Manager

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

# --- Constants ---
SRC_BUCKET = "investair-asx"
DEST_BUCKET = "xtf-asx"
MAX_RESTORES = 100
MAX_THREADS = 16  # Adjust based on your bandwidth & credentials

# --- Init ---
s3 = S3Manager()

# --- Thread-safe counters ---
lock = threading.Lock()
restored = 0
skipped = 0

def object_exists(bucket, key):
    """Check if object exists in the specified S3 bucket."""
    try:
        return key in s3.list_s3_objects(bucket)
    except Exception as e:
        logger.error(f"❌ Error checking existence of {key} in {bucket}: {e}")
        return False

def restore_single_pdf(key):
    global restored, skipped

    try:
        parts = key.split("/")
        if len(parts) < 5:
            logger.warning(f"⚠️ Skipping malformed key: {key}")
            with lock:
                skipped += 1
            return

        date_folder = parts[3]        # e.g. 20240528
        filename = parts[4]           # e.g. 02811489.pdf
        dest_key = f"{date_folder}/{filename}"

        if object_exists(DEST_BUCKET, dest_key):
            logger.info(f"⚠️ Already exists in destination: {dest_key}")
            with lock:
                skipped += 1
            return

        logger.info(f"🚚 Copying to {DEST_BUCKET}/{dest_key}")
        s3.s3.copy_object(
            Bucket=DEST_BUCKET,
            CopySource={"Bucket": SRC_BUCKET, "Key": key},
            Key=dest_key
        )
        logger.info(f"✅ Restored: {dest_key}")
        with lock:
            restored += 1

    except Exception as e:
        logger.error(f"❌ Failed to restore {key}: {e}")
        with lock:
            skipped += 1

def restore_pdfs():
    logger.info("🔍 Fetching all object keys from source bucket...")
    all_keys = s3.list_s3_objects(SRC_BUCKET)
    logger.info(f"📦 Total objects in {SRC_BUCKET}: {len(all_keys)}")

    pdf_keys = [k for k in all_keys if k.lower().endswith(".pdf") and k.count("/") >= 4]
    logger.info(f"📄 PDF candidates for restoration: {len(pdf_keys)}")

    # Limit to MAX_RESTORES
    pdf_keys = pdf_keys[:MAX_RESTORES]

    logger.info(f"🚀 Starting threaded restore of {len(pdf_keys)} files with {MAX_THREADS} threads...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(restore_single_pdf, key) for key in pdf_keys]
        for future in as_completed(futures):
            future.result()

    logger.info("✅ All threads complete.")
    logger.info(f"📊 Restore summary — Restored: {restored}, Skipped: {skipped}")
    return {"restored": restored, "skipped": skipped}

if __name__ == "__main__":
    restore_pdfs()
