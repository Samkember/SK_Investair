import os
import pandas as pd
from sqlalchemy import create_engine, text

# === CONFIGURATION ===
DB_HOST = "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com"
DB_USER = "sam"
DB_PASSWORD = "sam2025"
DB_NAME = "ASX_Market"
DB_PORT = 3306
DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SCHEMA = "ASX_Market"
CODES_TBL = "ASX_Company_Codes"
HOLD_TBL = "Substantial_Holding.reconciled"

# === OUTPUT DIRECTORY ===
OUTPUT_DIR = "/Users/samkember/Documents/SK_Investair/CustomTim/130625_SS_WeeklyReport/Outputs/sector_holdings"


# ---------------------------------------------------------------------------- #
def engine():
    return create_engine(DB_URL)


def all_sectors(conn) -> list[str]:
    """Fetch all unique sector names from the company codes table."""
    query = text(f"""
        SELECT DISTINCT Sector
        FROM {SCHEMA}.{CODES_TBL}
        WHERE Sector IS NOT NULL;
    """)
    return pd.read_sql(query, conn)["Sector"].tolist()


def tickers_for_sector(conn, sector: str) -> list[str]:
    """Get all tickers associated with a given sector."""
    query = text(f"""
        SELECT ticker
        FROM {SCHEMA}.{CODES_TBL}
        WHERE Sector = :sector;
    """)
    return pd.read_sql(query, conn, params={"sector": sector})["ticker"].tolist()


def holdings(conn, tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Query substantial holding records for given tickers and date range."""
    if not tickers:
        return pd.DataFrame()

    bind_names = [f":t{i}" for i in range(len(tickers))]
    bind_clause = ",".join(bind_names)

    query = text(f"""
        SELECT id,
               shareholder,
               ticker,
               event_date,
               event_type,
               prev_voting_power,
               post_voting_power
        FROM {HOLD_TBL}
        WHERE ticker IN ({bind_clause})
          AND event_date BETWEEN :start AND :end;
    """)

    params = {f"t{i}": t for i, t in enumerate(tickers)}
    params.update({"start": start_date, "end": end_date})

    return pd.read_sql(query, conn, params=params)


def sanitize_filename(name: str) -> str:
    """Make sector name safe for filenames."""
    return name.replace(" ", "_").replace("&", "and").replace("/", "_").replace("(", "").replace(")", "").replace(",", "")


def run_for_all_sectors(start_date: str, end_date: str):
    """Run holdings queries for each sector and save to individual CSVs."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = engine()
    sector_count = 0

    for sector in all_sectors(conn):
        tickers = tickers_for_sector(conn, sector)
        df = holdings(conn, tickers, start_date, end_date)
        if df.empty:
            continue

        filename = sanitize_filename(sector) + ".csv"
        output_path = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(output_path, index=False)
        print(f"✅ Saved {len(df)} rows for '{sector}' to {output_path}")
        sector_count += 1

    if sector_count == 0:
        print("⚠️ No data found for any sector in the specified range.")

    conn.dispose()


# ---------------------------------------------------------------------------- #
if __name__ == "__main__":
    run_for_all_sectors("2025-05-05", "2025-05-09")
