import pandas as pd
from pathlib import Path

from ASX_Codes import get_tickers_by_sector
from Get_files_for_companies import get_files_for_tickers
from s3_manager import S3Manager
from extraction import extract_from_pdf_bytes

# Set output directory
output_file_path = r"C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms\ASX_SS_Forms_TS\output.csv"
output_dir = Path(output_file_path).parent
failed_pdf_dir = output_dir / "failed_pdfs"
failed_pdf_dir.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    # Step 1: Get ASX tickers in Capital Goods sector
    capgoods = get_tickers_by_sector("Capital Goods")
    print(f"Found {len(capgoods)} Capital Goods companies.")

    # Step 2: Get all matching announcement files
    files_df = get_files_for_tickers(
        tickers_df=capgoods,
        rep_type_codes=["02002"], #02001 = Becoming, 02002 = change 02003 = cease 
        mysql_url=None
    )

    matched = files_df.dropna(subset=["Filename"])
    
    print(matched)

    if matched.empty:
        print("\nNo matching files found.")
        exit()

    print(f"\nFound {len(matched)} matching files.")

    # Step 3: Initialize
    s3m = S3Manager()
    bucket = "xtf-asx"

    extracted_data = []
    failed_files = []

    # Step 4: Loop through each file
    for i, row in matched.iterrows():
        full_id = row["Filename"]
        folder = full_id[:8]
        file_id = full_id[8:]
        key = f"{folder}/{file_id}.pdf"

        try:
            pdf_bytes = s3m.get_file_bytes(bucket_name=bucket, key=key)
            if not pdf_bytes:
                print(f"[!] Empty or missing: {key}")
                failed_files.append((full_id, row["Ticker"], "Empty or missing"))
                continue

            data = extract_from_pdf_bytes(pdf_bytes)
            data["Filename"] = full_id
            data["Ticker"] = row["Ticker"]
            extracted_data.append(data)

            print(f"[✓] Extracted {key}")

        except Exception as e:
            print(f"[✗] Failed {full_id}: {e}")
            failed_files.append((full_id, row["Ticker"], str(e)))

    # Step 5: Save successful extractions
    extracted_df = pd.DataFrame(extracted_data)
    extracted_df.to_csv(output_file_path, index=False)
    print(f"\n[✔] Saved extracted data to {output_file_path}")

    # Step 6: Save failed file list
    failed_df = pd.DataFrame(failed_files, columns=["Filename", "Ticker", "Reason"])
    failed_csv_path = output_file_path.replace(".csv", "_failed.csv")
    failed_df.to_csv(failed_csv_path, index=False)
    print(f"[✗] Saved failed file list to {failed_csv_path}")

    # Step 7: Download failed files for manual inspection
    for fname, ticker, _ in failed_files:
        folder = fname[:8]
        file_id = fname[8:]
        key = f"{folder}/{file_id}.pdf"
        local_path = failed_pdf_dir / f"{ticker}_{fname}.pdf"

        try:
            s3m.s3.download_file(bucket, key, str(local_path))
            print(f"[↓] Downloaded {key} → {local_path.name}")
        except Exception as e:
            print(f"[!] Could not download {key}: {e}")