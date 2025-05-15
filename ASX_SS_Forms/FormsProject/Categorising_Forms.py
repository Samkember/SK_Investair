import pandas as pd
from sqlalchemy import create_engine, text
from s3_manager import S3Manager
import os
import pdfplumber
import pytesseract
from pdf2image import convert_from_bytes
import logging
import re

import csv
 

from datetime import datetime

from s3_manager import S3Manager 

from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, text
import pymysql

# Suppress verbose pdfminer warnings
logging.getLogger("pdfminer").setLevel(logging.ERROR)

# Define a dictionary for RepType Codes if needed
REP_CODES = {
    '02001': 'Becoming substantial holder',
    '02002': 'Change in substantial holding',
    '02003': 'Ceasing to be substantial holder',
    '03002': 'Top 20 shareholders'
    # Add more codes here if needed
}

def extract_ticker(lines):
    """
    Extract ASX ticker: 3–5 alphanumeric chars with at least one letter.
    """
    for line in lines:
        if re.fullmatch(r'[A-Z0-9]{3}', line) and re.search(r'[A-Z]', line):
            return line
    return "N/A"


def parse_header_file(content):
    """
    Parses ASX header .txt file content.
    Returns: dict with ticker and rep type codes.
    """
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    return {
        "ticker": extract_ticker(lines),
        "rep_type_codes": re.findall(r"\b\d{5}\b", content)
    }


def identify_substantial_holdings(s3_manager, bucket_name, save_every=200, output_dir="output_batches"):
    """
    Scans .txt files from S3, extracts ASX ticker, RepType code, and date from filename.
    Saves every `save_every` matches to CSV in `output_dir`.
    Returns: full matched DataFrame
    """
    os.makedirs(output_dir, exist_ok=True)
    object_keys = s3_manager.list_s3_objects(bucket_name)

    matched_records = []
    match_count = 0
    batch_num = 1

    for key in object_keys:
        if not key.endswith('.txt'):
            continue

        content = s3_manager.get_object_content(bucket_name, key)
        if content is None:
            continue

        parsed = parse_header_file(content)
        found_code = next((code for code in parsed["rep_type_codes"] if code in REP_CODES), None)

        if found_code:
            cleaned_filename = key.replace('/', '').replace('.txt', '')
            parsed_ticker = parsed['ticker']

            try:
                parsed_date = datetime.strptime(key[:8], "%Y%m%d").date()
            except ValueError:
                parsed_date = "N/A"

            matched_records.append({
                'Filename': cleaned_filename,
                'RepType Code': found_code,
                'Description': REP_CODES[found_code],
                'Ticker': parsed_ticker,
                'Date': parsed_date
            })

            match_count += 1

            if match_count % save_every == 0:
                df = pd.DataFrame(matched_records[-save_every:])
                output_path = os.path.join(output_dir, f"substantial_holdings_batch_{batch_num}.csv")
                df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)
                print(f"📁 Saved batch {batch_num} with {save_every} matches → {output_path}")
                batch_num += 1

    # Save remaining matches
    remainder = match_count % save_every
    if remainder:
        df = pd.DataFrame(matched_records[-remainder:])
        output_path = os.path.join(output_dir, f"substantial_holdings_batch_{batch_num}.csv")
        df.to_csv(output_path, index=False)
        print(f"📁 Saved final batch {batch_num} with {remainder} matches → {output_path}")

    return pd.DataFrame(matched_records)

if __name__ == "__main__":
    bucket_name = "xtf-asx"
    s3m = S3Manager()

    # Step 1: Extract and save to CSV
    holdings_df = identify_substantial_holdings(s3m, bucket_name, save_every=50)

    print("\n✅ Files related to substantial holdings:")
    print(holdings_df)

    csv_path = "substantial_holdings_report.csv"
    holdings_df.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
    print(f"📄 Saved report to: {csv_path}")

    # Step 2: Upload to MySQL
    try:
        # Convert date column to proper format
        holdings_df["Date"] = pd.to_datetime(holdings_df["Date"]).dt.date
        upload_date = holdings_df["Date"].min()

        # SQL connection
        engine = create_engine(
            'mysql+pymysql://sam:sam2025@database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/ASX_Market'
        )
        meta = MetaData()

        with engine.begin() as conn:
            if not engine.dialect.has_table(conn, "ASX_RepTypes", schema="ASX_Market"):
                rep_table = Table(
                    'ASX_RepTypes', meta,
                    Column('Filename', String(50)),
                    Column('RepType Code', String(10)),
                    Column('Description', String(100)),
                    Column('Ticker', String(10)),
                    Column('Date', Date),
                    schema='ASX_Market'
                )
                meta.create_all(engine)
                print("📋 Created SQL table 'ASX_RepTypes' in schema 'ASX_Market'.")

            # Delete existing rows for the same date
            delete_query = text("""
                DELETE FROM ASX_Market.ASX_RepTypes
                WHERE `Date` = :upload_date
            """)
            conn.execute(delete_query, {"upload_date": upload_date})
            print(f"🧹 Removed existing entries for date {upload_date}")

        # Upload new data
        with engine.begin() as conn:
            holdings_df.to_sql(
                name='ASX_RepTypes',
                con=conn,
                if_exists='append',
                index=False,
                schema='ASX_Market'
            )
            print("✅ Upload to SQL complete.")

    except Exception as e:
        print(f"❌ Failed to upload to SQL: {e}")