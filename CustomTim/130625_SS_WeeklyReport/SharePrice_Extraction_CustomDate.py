import os
import pandas as pd
from sqlalchemy import create_engine, text

# --------------------------------------------------------------------------- #
# === DATABASE CONFIGURATION ===
DB_HOST = "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com"
DB_USER = "sam"
DB_PASSWORD = "sam2025"
DB_NAME = "ASX_Market"
DB_PORT = 3306
MYSQL_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SCHEMA = "ASX_Market"
CODES_TBL = "ASX_Company_Codes"
SNAPSHOT_TBL = "asx_market_snapshot"

# === OUTPUT PATH TEMPLATE ===
CSV_OUTPUT_TEMPLATE = (
    "/Users/samkember/Documents/SK_Investair/CustomTim/130625_SS_WeeklyReport/"
    "Outputs/CapitalGoods_prices_{start}_{end}.csv"
)
# --------------------------------------------------------------------------- #


def get_engine():
    return create_engine(MYSQL_URL)


def get_capital_goods_tickers(conn) -> list[str]:
    q = text(f"SELECT ticker FROM {SCHEMA}.{CODES_TBL} WHERE Sector = 'Capital Goods';")
    return pd.read_sql(q, conn)["ticker"].tolist()


def fetch_snapshot_data(conn, tickers, start_date, end_date) -> pd.DataFrame:
    if not tickers:
        print("⚠️  No Capital Goods tickers found.")
        return pd.DataFrame()

    placeholders = [f":t{i}" for i in range(len(tickers))]
    query = text(f"""
        SELECT ticker, snapshot_date, market_cap, beta, open_price, high, low,
               close_price, adjusted_close, volume,
               ema_50d, ema_200d, hi_250d, lo_250d,
               avgvol_14d, avgvol_50d, avgvol_200d
        FROM {SCHEMA}.{SNAPSHOT_TBL}
        WHERE ticker IN ({','.join(placeholders)})
          AND snapshot_date BETWEEN :start AND :end
        ORDER BY ticker, snapshot_date;
    """)

    params = {f"t{i}": t for i, t in enumerate(tickers)}
    params.update({"start": start_date, "end": end_date})
    return pd.read_sql(query, conn, params=params)


def main(start_date: str, end_date: str):
    conn = get_engine()
    tickers = get_capital_goods_tickers(conn)
    df = fetch_snapshot_data(conn, tickers, start_date, end_date)

    if df.empty:
        print(f"No snapshot data found for {start_date} to {end_date}.")
    else:
        print(df)
        print(f"\n✅ Retrieved {len(df)} rows for {df['ticker'].nunique()} tickers.")

        output_path = CSV_OUTPUT_TEMPLATE.format(start=start_date, end=end_date)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"📄 CSV saved to: {output_path}")

    conn.dispose()


# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    # 🟢 Set your custom date range here
    start_date = "2025-05-05"
    end_date = "2025-05-09"

    main(start_date, end_date)
