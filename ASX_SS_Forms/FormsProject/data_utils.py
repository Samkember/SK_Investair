import pandas as pd
from sqlalchemy import create_engine, text
from s3_manager import S3Manager
import os

def get_all_tickers(sector_filter=None):
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    df = pd.read_csv(url, skiprows=1)

    possible_sector_cols = ["Industry Group", "GICS industry group", "Industry", "GICS Sector"]
    sector_col = next((col for col in possible_sector_cols if col in df.columns), None)

    if sector_col:
        df["Sector"] = df[sector_col].str.strip()
    else:
        df["Sector"] = "Unknown"

    df["Ticker"] = df["ASX code"].str.strip().str.upper()
    df["Company"] = df["Company name"]

    # Apply sector filter if specified
    if sector_filter:
        df = df[df["Sector"].str.lower() == sector_filter.lower().strip()]

    return df[["Ticker", "Company", "Sector"]]


def get_document_numbers(ASX_ticker):
    """Query SQL to get clean list of 16-digit refs for a given ticker"""
    try:
        engine = create_engine(
            'mysql+pymysql://sam:sam2025@database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/ASX_Market'
        )

        query = text("""
            SELECT refs FROM Feed_Public.feed_new
            WHERE primary_stock = :ticker
        """)

        with engine.connect() as conn:
            result = conn.execute(query, {"ticker": ASX_ticker})
            raw_refs = [
                str(row[0]).strip().replace("[", "").replace("]", "").replace('"', '')
                for row in result.fetchall()
            ]
            df = pd.DataFrame(raw_refs, columns=["refs"])

        print(f"✅ Retrieved {len(df)} document refs for {ASX_ticker}")
        return df

    except Exception as e:
        print(f"❌ Failed to retrieve document refs: {e}")
        return pd.DataFrame()



def find_s3_docs_by_refs(ticker, s3_manager, bucket_name, download=False, local_dir="downloads"):
    """Match and optionally download .pdf S3 documents for a ticker's ref list"""
    ref_df = get_document_numbers(ticker)

    if ref_df.empty:
        print(f"⚠️ No refs found for ticker {ticker}")
        return []

    # Step 1: Clean ref column to flat list of 16-digit strings
    refs = ref_df["refs"].astype(str).str.strip().str.zfill(16).tolist()
    found_docs = []
    all_keys = s3_manager.list_s3_objects(bucket_name)

    print(f"📦 S3 key count: {len(all_keys)}")

    for ref in ref_df["refs"]:
        ref = str(ref).strip()
        if len(ref) != 16:
            continue  # skip invalid ref lengths

        folder = ref[:8]         # YYYYMMDD
        filename = ref[8:]       # Document ID
        s3_key = f"{folder}/{filename}.pdf"

        print(f"🔍 Checking: {s3_key}")  # This should now look perfect

        if s3_key in all_keys:
            found_docs.append(s3_key)

            if download:
                os.makedirs(os.path.join(local_dir, ticker), exist_ok=True)
                local_path = os.path.join(local_dir, ticker, f"{filename}.pdf")
                s3_manager.download_file(object_name=s3_key, bucket_name=bucket_name, file_name=local_path)


    print(f"✅ Found {len(found_docs)} .pdf documents for ticker {ticker}")
    return found_docs