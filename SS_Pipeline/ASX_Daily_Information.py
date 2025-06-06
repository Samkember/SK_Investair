# === IMPORTS ===
import os
import threading
import pandas as pd
import cloudscraper
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, Table, Column, MetaData, String, Float, text, Date
from dateutil.relativedelta import relativedelta
import time

# === CONFIG ===
OUTPUT_FOLDER = "Full ASX Snapshot"
APP_ID = "af5f4d73c1a54a33"
MAX_MARKETINDEX_RETRIES = 2
NUM_THREADS_MARKETINDEX = 10
DATE_USED = datetime.now(timezone(timedelta(hours=10))).date()
SCHEMA_NAME = "ASX_Market"
TICKER_TABLE = "ASX_Company_Codes"
DATA_TABLE = "ASX_DailyMarketInformation"

# === GLOBALS ===
print_lock = threading.Lock()
result_lock = threading.Lock()
session = cloudscraper.create_scraper()

# === COLUMN FIX ===
def standardize_column_names(df):
    rename_map = {
        "company": "company_name",
        "change_pct": "pct_change"
    }
    return df.rename(columns=rename_map)

# === FETCH HISTORICAL RETURNS ===
def fetch_Historical_Returns(ticker: str, app_id: str = APP_ID):
    url = f"https://quoteapi.com/api/v5/symbols/{ticker.lower()}.asx/ticks"
    headers = {
        "accept": "application/json",
        "referer": f"https://www.marketindex.com.au/asx/{ticker.lower()}?src=search-all"
    }
    range_options = ["20y", "5y", "1y", "6m", "3m"]

    for range_ in range_options:
        try:
            params = {
                "appID": app_id,
                "adjustment": "capital",
                "fields": "dc",
                "range": range_
            }
            response = session.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            ticks = data.get("ticks", {})
            dates = ticks.get("date", [])
            closes = ticks.get("close", [])

            if not dates or not closes or len(dates) != len(closes):
                continue

            df = pd.DataFrame({"Date": dates, "Price": closes})
            df["Date"] = pd.to_datetime(df["Date"])
            df.set_index("Date", inplace=True)
            df.sort_index(inplace=True)
            df = df[df["Price"].notnull()]
            if df.empty:
                continue

            latest_price = df["Price"].iloc[-1]
            today = df.index[-1].replace(hour=0, minute=0, second=0, microsecond=0)

            offsets = {
                "return_1m": today - relativedelta(months=1),
                "return_3m": today - relativedelta(months=3),
                "return_6m": today - relativedelta(months=6),
                "return_12m": today - relativedelta(months=12),
                "return_3y": today - relativedelta(years=3),
                "return_5y": today - relativedelta(years=5),
            }

            returns = {}
            for label, target_date in offsets.items():
                valid_dates = df.index[df.index <= target_date]
                if not valid_dates.empty:
                    closest_date = valid_dates.max()
                    past_price = df.loc[closest_date, "Price"]
                    returns[label] = round(((latest_price - past_price) / past_price) * 100, 2)
                else:
                    returns[label] = None

            return returns

        except Exception:
            continue

    return {label: None for label in [
        "return_1m", "return_3m", "return_6m", "return_12m", "return_3y", "return_5y"]}

# === FETCH MARKETINDEX SNAPSHOT ===
def fetch_marketindex_data(ticker):
    ticker_lower = ticker.lower()
    url = f"https://quoteapi.com/api/v5/symbols/{ticker_lower}.asx"
    params = {
        "appID": APP_ID,
        "averages": "1",
        "fundamentals": "1",
        "liveness": "overnight"
    }
    headers = {
        "accept": "application/json",
        "user-agent": "Mozilla/5.0",
        "referer": f"https://www.marketindex.com.au/asx/{ticker_lower}"
    }
    try:
        response = session.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        quote = data.get("quote", {})
        fundamentals = data.get("fundamentals", {})
        desc = data.get("desc", {})

        open_price = quote.get("open") or quote.get("price")
        close_price = quote.get("close") or quote.get("price")
        volume = quote.get("volume")
        shares_issued = fundamentals.get("sharesIssued")
        market_cap = round(float(shares_issued) * float(close_price), 2) if shares_issued and close_price else None
        pct_change = round(((float(close_price) - float(open_price)) / float(open_price)) * 100, 2) if open_price and close_price else None

        returns = fetch_Historical_Returns(ticker)

        return {
            "info_date": DATE_USED,
            "ticker": ticker.upper(),
            "company": desc.get("issuerName", "Unknown"),
            "sector": desc.get("industryGroup", "Unknown"),
            "market_cap": market_cap,
            "shares_outstanding": shares_issued,
            "open_price": open_price,
            "close_price": close_price,
            "volume": volume,
            "change_pct": pct_change,
            **returns
        }
    except Exception:
        return None

# === MAIN PIPELINE ===
def run_combined_pipeline():
    engine = create_engine(
        f'mysql+pymysql://sam:sam2025@database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/{SCHEMA_NAME}'
    )
    query = f"SELECT Ticker FROM {SCHEMA_NAME}.{TICKER_TABLE}"
    df_listed = pd.read_sql(query, con=engine)
    tickers = df_listed["Ticker"].dropna().str.strip().str.upper().unique().tolist()

    marketindex_results = []
    marketindex_failed = []

    def process_marketindex(ticker):
        for attempt in range(MAX_MARKETINDEX_RETRIES):
            result = fetch_marketindex_data(ticker)
            if result:
                with result_lock:
                    marketindex_results.append(result)
                print(f"✅ MarketIndex: {ticker}")
                return
            time.sleep(1 + attempt)
        with result_lock:
            marketindex_failed.append(ticker)
        print(f"❌ MarketIndex failed: {ticker}")

    with ThreadPoolExecutor(max_workers=NUM_THREADS_MARKETINDEX) as executor:
        executor.map(process_marketindex, tickers)

    if marketindex_failed:
        print(f"🔁 Retrying {len(marketindex_failed)} failed tickers...")
        retry_list = marketindex_failed[:]
        marketindex_failed.clear()
        with ThreadPoolExecutor(max_workers=NUM_THREADS_MARKETINDEX) as executor:
            executor.map(process_marketindex, retry_list)

    df_combined = pd.DataFrame(marketindex_results)
    df_combined["info_date"] = pd.to_datetime(df_combined["info_date"]).dt.date
    df_combined = standardize_column_names(df_combined)
    upload_date = df_combined["info_date"].iloc[0]

    print(f"📦 Preparing to upload {len(df_combined)} rows for {upload_date}")

    try:
        meta = MetaData()
        with engine.begin() as conn:
            if not engine.dialect.has_table(conn, DATA_TABLE, schema=SCHEMA_NAME):
                asx_table = Table(
                    DATA_TABLE, meta,
                    Column('info_date', Date),
                    Column('ticker', String(10)),
                    Column('company_name', String(255)),
                    Column('sector', String(100)),
                    Column('market_cap', Float),
                    Column('shares_outstanding', Float),
                    Column('open_price', Float),
                    Column('close_price', Float),
                    Column('volume', Float),
                    Column('pct_change', Float),
                    Column('return_1m', Float),
                    Column('return_3m', Float),
                    Column('return_6m', Float),
                    Column('return_12m', Float),
                    Column('return_3y', Float),
                    Column('return_5y', Float),
                    schema=SCHEMA_NAME
                )
                meta.create_all(engine)
                print(f"📋 SQL table '{DATA_TABLE}' created in schema {SCHEMA_NAME}.")

            conn.execute(
                text(f"DELETE FROM {SCHEMA_NAME}.{DATA_TABLE} WHERE info_date = :upload_date"),
                {"upload_date": upload_date}
            )
            print(f"🗑️ Deleted rows for date {upload_date}")

        with engine.begin() as conn:
            df_combined.to_sql(
                name=DATA_TABLE,
                con=conn,
                if_exists='append',
                index=False,
                schema=SCHEMA_NAME
            )
            print("✅ Upload to SQL complete!")

    except Exception as e:
        print(f"❌ Failed to upload to SQL: {e}")


# === ENTRY POINT ===
if __name__ == "__main__":
    start_time = time.time()
    run_combined_pipeline()




# === LAMBDA HANDLER ===

# def lambda_handler(event=None, context=None):
#     run_combined_pipeline()
#     return {
#         "statusCode": 200,
#         "body": "ASX data pipeline completed successfully."
#     }
