from s3_manager import S3Manager

# --- Bucket Names ---
SRC_BUCKET = "xtf-asx"
DEST_BUCKET = "substantial-holding"

def count_file_types():
    s3 = S3Manager()

    # Get all keys from both buckets
    investair_keys = s3.list_s3_objects(DEST_BUCKET)
    # xtf_keys = s3.list_s3_objects(SRC_BUCKET)

    # Count .pdfs in investair-asx
    investair_pdfs = [k for k in investair_keys if k.lower().endswith(".pdf")]
    print(f"📦 investair-asx PDF count: {len(investair_pdfs)}")

    # Count .pdfs in xtf-asx
    # xtf_pdfs = [k for k in xtf_keys if k.lower().endswith(".pdf")]
    # print(f"📦 xtf-asx PDF count: {len(xtf_pdfs)}")

    # Count .txts in xtf-asx
    # xtf_txts = [k for k in xtf_keys if k.lower().endswith(".txt")]
    # print(f"📄 xtf-asx TXT count: {len(xtf_txts)}")

if __name__ == "__main__":
    count_file_types()

