import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, text

# === CONFIG ===
SCHEMA_NAME = "ASX_Market"
TABLE_NAME = "ASX_Company_Codes"

def get_tickers_by_sector(sectors=None):
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    df = pd.read_csv(url, skiprows=1)

    possible = ["Industry Group", "GICS industry group", "Industry", "GICS Sector"]
    sector_col = next((c for c in possible if c in df.columns), None)
    df["Sector"] = df[sector_col].astype(str).str.strip() if sector_col else "Unknown"

    df["Ticker"] = df["ASX code"].astype(str).str.upper().str.strip()
    df["CompanyName"] = df["Company name"].astype(str).str.strip()

    if sectors is None or (isinstance(sectors, str) and sectors.lower() == "all"):
        mask = pd.Series(True, index=df.index)
    else:
        wanted = {sectors} if isinstance(sectors, str) else set(sectors)
        mask = df["Sector"].isin(wanted)
        
    df['Success'] = ''

    return df.loc[mask, ["Ticker", "CompanyName", "Sector"]].reset_index(drop=True)


def SQL_upload(df):
    """
    Uploads a DataFrame of ASX tickers to the MySQL table `ASX_Company_Codes`
    in the `ASX_Market` schema. Creates the table if it does not exist.

    Parameters:
        df (pd.DataFrame): DataFrame with columns ['Ticker', 'CompanyName', 'Sector']
    """
    try:
        engine = create_engine(
            f'mysql+pymysql://sam:sam2025@database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/{SCHEMA_NAME}'
        )
        meta = MetaData()

        with engine.begin() as conn:
            if not engine.dialect.has_table(conn, TABLE_NAME, schema=SCHEMA_NAME):
                asx_codes_table = Table(
                    TABLE_NAME, meta,
                    Column('Ticker', String(10), primary_key=True),
                    Column('CompanyName', String(255)),
                    Column('Sector', String(100)),
                    Column('Sucess', String(100)),
                    schema=SCHEMA_NAME
                )
                meta.create_all(engine)
                print(f"📋 SQL table '{TABLE_NAME}' created in schema {SCHEMA_NAME}.")

            # Optional: Clear table first
            conn.execute(text(f"DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}"))

        with engine.begin() as conn:
            df.to_sql(
                name=TABLE_NAME,
                con=conn,
                if_exists='append',
                index=False,
                schema=SCHEMA_NAME
            )
            print(f"✅ ASX company codes uploaded to {SCHEMA_NAME}.{TABLE_NAME}!")

    except Exception as e:
        print(f"❌ Failed to upload ASX company codes: {e}")


if __name__ == "__main__":
    df = get_tickers_by_sector()
    SQL_upload(df)
