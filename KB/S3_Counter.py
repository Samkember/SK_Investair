from s3_manager import S3Manager

# --- Global Config ---
# BUCKET_NAME = "investair-asx"
BUCKET_NAME = "xtf-asx"

def count_pdf_files(bucket_name):
    s3 = S3Manager()
    all_keys = s3.list_s3_objects(bucket_name)
    pdf_keys = [key for key in all_keys if key.lower().endswith(".pdf")]
    
    print(f"📄 Total PDF files in '{bucket_name}': {len(pdf_keys)}")

    # Extract base filenames (ignore folder structure, remove .pdf, lowercase)
    base_names = [key.split("/")[-1].replace(".pdf", "").lower() for key in pdf_keys]
    
    unique_count = len(set(base_names))
    total_count = len(base_names)

    if unique_count == total_count:
        print("✅ All PDF filenames are unique.")
    else:
        print(f"⚠️ {total_count - unique_count} duplicate filename(s) detected (ignoring folder structure).")

    return total_count

if __name__ == "__main__":
    count_pdf_files(BUCKET_NAME)
