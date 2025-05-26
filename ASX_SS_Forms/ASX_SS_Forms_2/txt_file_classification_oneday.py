import os, sys
from s3_manager import S3Manager
import pandas as pd 

bucket = "xtf-asx"
prefix = "20250526/"
outputFolderPath =  r"C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms\ASX_SS_Forms_2\Output"
outputFileName = f"ASX_SS_Classified_{prefix.strip('/')}.csv"


# === Main Parser ===
def parse_txt_header(fields, filename):
    rep_types = [f.strip() for f in fields[7:27] if f.strip()]  # Get non-empty classification codes

    return {
        "filename": filename,
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
        "rep_types": rep_types
    }

def extract_file_codes(line):
    codes = []
    for val in line[7:]:
        if val.strip() == "":
            break
        codes.append(val.strip())
    
    return codes

def list_txt_files():
    s3 = S3Manager()
    txt_files = s3.list_files(bucket_name=bucket, folder=prefix)
    txt_files = [f for f in txt_files if f.endswith(".txt")]
    print(f"🔍 Found {len(txt_files)} .txt files to process.")

    all_records = []

    count = 0
    for file in txt_files:
        raw_output = s3.get_file_bytes(bucket_name=bucket, key=file)
        decoded = raw_output.decode('utf-8')
        fields = decoded.splitlines()

        if not fields or len(fields) < 37:
            print(f"⚠️ Skipping malformed file: {file}")
            continue

        try:
            record = parse_txt_header(fields, file)
            all_records.append(record)
        except Exception as e:
            print(f"❌ Error parsing {file}: {e}")
            continue

    # Convert to DataFrame
    df = pd.DataFrame(all_records)

    # Convert rep_types list to stringified list for SQL-friendly storage
    df["rep_types"] = df["rep_types"].apply(lambda x: str(x))

    # Save to CSV
    output_path = os.path.join(outputFolderPath, outputFileName)
    df.to_csv(output_path, index=False)
    print(f"✅ Saved {len(df)} classified announcements to {output_path}")

if __name__ == "__main__":
    list_txt_files()
