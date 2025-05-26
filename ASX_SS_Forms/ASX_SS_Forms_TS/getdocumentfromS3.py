import os
from s3_manager import S3Manager

output_folder = r"C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms\DocumentDownloadFolder"

def fetch_document(file_code: str):
    if len(file_code) != 16 or not file_code.isdigit():
        raise ValueError("File code must be a 16-digit numeric string.")

    folder = file_code[:8]
    filename = file_code[8:]
    key = f"{folder}/{filename}.pdf"

    bucket = "xtf-asx"

    # Instantiate S3 manager and download file
    s3 = S3Manager()

    # Ensure the full file path is specified
    download_path = os.path.join(output_folder, f"{filename}.pdf")

    s3.download_file(object_name=key, bucket_name=bucket, file_name=download_path)
    print(f"File downloaded successfully to {download_path}")

if __name__ == "__main__":
    file_code = "2025052202949083"
    fetch_document(file_code)