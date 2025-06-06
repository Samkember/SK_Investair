import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, text
from datetime import datetime

# === CONFIGURATION ===
SCHEMA_NAME = "ASX_Market"
TABLE_NAME = "ASX_Company_Codes"
ASX_CSV_URL = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"


# === Load and Filter ASX Company Data ===
def get_tickers_by_sector(sectors=None):
    # print("📥 Downloading ASX listed companies CSV...")
    try:
        df = pd.read_csv(ASX_CSV_URL, skiprows=1)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to load ASX CSV: {e}")

    # Identify the correct sector column
    possible_sector_cols = ["Industry Group", "GICS industry group", "Industry", "GICS Sector"]
    sector_col = next((c for c in df.columns if c in possible_sector_cols), None)
    if not sector_col:
        raise ValueError("❌ No known sector column found in ASX CSV.")

    # print(f"📊 Using sector column: {sector_col}")

    # Clean and standardize data
    df["Sector"] = df[sector_col].astype(str).str.strip()
    df["Ticker"] = df["ASX code"].astype(str).str.upper().str.strip()
    df["CompanyName"] = df["Company name"].astype(str).str.strip()
    df["UploadDate"] = pd.to_datetime(datetime.now().date())

    # Filter by sector if specified
    if sectors is None or (isinstance(sectors, str) and sectors.lower() == "all"):
        mask = pd.Series(True, index=df.index)
    else:
        wanted = {sectors} if isinstance(sectors, str) else set(sectors)
        mask = df["Sector"].isin(wanted)

    filtered_df = df.loc[mask, ["Ticker", "CompanyName", "Sector", "UploadDate"]].reset_index(drop=True)
    # print(f"✅ Retrieved {len(filtered_df)} companies")
    return filtered_df


# === Upload to MySQL Database ===
def SQL_upload(df):
    # print("🔌 Connecting to MySQL...")
    try:
        engine = create_engine(
            f'mysql+pymysql://sam:sam2025@database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/{SCHEMA_NAME}'
        )
        meta = MetaData()

        with engine.begin() as conn:
            if not engine.dialect.has_table(conn, TABLE_NAME, schema=SCHEMA_NAME):
                print(f"📦 Table '{TABLE_NAME}' not found. Creating...")
                asx_codes_table = Table(
                    TABLE_NAME, meta,
                    Column('Ticker', String(10), primary_key=True),
                    Column('CompanyName', String(255)),
                    Column('Sector', String(100)),
                    Column('UploadDate', Date),
                    schema=SCHEMA_NAME
                )
                meta.create_all(engine)
                print(f"✅ Table '{TABLE_NAME}' created.")

            # print(f"🧹 Clearing existing data in {SCHEMA_NAME}.{TABLE_NAME}...")
            conn.execute(text(f"DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}"))

        with engine.begin() as conn:
            df.to_sql(
                name=TABLE_NAME,
                con=conn,
                if_exists='append',
                index=False,
                schema=SCHEMA_NAME
            )
            print(f"✅ Uploaded {len(df)} records to {SCHEMA_NAME}.{TABLE_NAME}")

    except Exception as e:
        raise RuntimeError(f"❌ Database operation failed: {e}")


# === Main Pipeline Runner ===
def run_combined_pipeline():
    # print("🚀 Starting ASX company upload pipeline...")
    df = get_tickers_by_sector("all")
    SQL_upload(df)
    # print("🏁 Pipeline completed.")


# def lambda_handler(event, context):
#     run_combined_pipeline()
#     return {
#         'statusCode': 200,
#         "body": "ASX data pipeline completed successfully."
#     }


if __name__ == "__main__";
    run_combined_pipeline()
    print("ASX data pipeline completed successfully.")