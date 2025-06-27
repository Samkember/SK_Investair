from s3_manager import S3Manager

# --- Global Config ---
BUCKET_NAME = "investair-asx"

def count_pdf_files(bucket_name):
    s3 = S3Manager()
    all_keys = s3.list_s3_objects(bucket_name)
    pdf_files = [key for key in all_keys if key.lower().endswith(".pdf")]
    print(f"📄 Total PDF files in '{bucket_name}': {len(pdf_files)}")
    return len(pdf_files)

if __name__ == "__main__":
    count_pdf_files(BUCKET_NAME)
