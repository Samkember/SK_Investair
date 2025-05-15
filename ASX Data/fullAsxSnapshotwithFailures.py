# === IMPORTS ===
import os
import time
import threading
import pandas as pd
import cloudscraper
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, Table, Column, MetaData, String, Float

# === CONFIG ===
OUTPUT_FOLDER = "Full ASX Snapshot"
APP_ID = "af5f4d73c1a54a33"
MAX_MARKETINDEX_RETRIES = 3
NUM_THREADS_MARKETINDEX = 10

# === GLOBALS ===
print_lock = threading.Lock()
result_lock = threading.Lock()
session = cloudscraper.create_scraper()

# === SECTOR MAP ===
def get_sector_map():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    df = pd.read_csv(url, skiprows=1)
    possible_sector_cols = ["Industry Group", "GICS industry group", "Industry", "GICS Sector"]
    sector_col = next((col for col in possible_sector_cols if col in df.columns), None)
    df["Sector"] = df[sector_col] if sector_col else "Unknown"
    df["Ticker"] = df["ASX code"].str.strip().str.upper()
    return df.set_index("Ticker")["Sector"].to_dict()

SECTOR_MAP = get_sector_map()

# === FETCH FROM MARKETINDEX ===
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
        "accept-language": "en-US,en;q=0.9",
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
        sector = SECTOR_MAP.get(ticker.upper(), "Unknown")

        return {
            "Date": datetime.now().strftime("%Y-%m-%d"),
            "Ticker": ticker.upper(),
            "Company": desc.get("issuerName", "Unknown"),
            "Sector": sector,
            "Market Cap": market_cap,
            "Shares Outstanding": shares_issued,
            "Open": open_price,
            "Close": close_price,
            "Volume": volume,
            "Change (%)": pct_change,
            "1 Month Return (%)": None,
            "3 Month Return (%)": None,
            "6 Month Return (%)": None,
            "12 Month Return (%)": None,
            "3 Year Return (%)": None,
            "5 Year Return (%)": None
        }
    except Exception:
        return None

# === MAIN RUN FUNCTION ===
def run_combined_pipeline():
    # Load ticker list
    df_listed = pd.read_csv("https://www.asx.com.au/asx/research/ASXListedCompanies.csv", skiprows=1)
    df_listed = df_listed[["ASX code", "Company name"]].dropna()
    df_listed.columns = ["asx_code", "Company"]
    tickers = df_listed["asx_code"].str.strip().str.upper().tolist()

    # Containers
    marketindex_results = []
    marketindex_failed = []

    # Fetch function for threads
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

    # Initial run
    with ThreadPoolExecutor(max_workers=NUM_THREADS_MARKETINDEX) as executor:
        executor.map(process_marketindex, tickers)

    # Retry failed ones
    if marketindex_failed:
        print(f"🔁 Retrying {len(marketindex_failed)} failed tickers...")
        retry_list = marketindex_failed[:]
        marketindex_failed.clear()
        with ThreadPoolExecutor(max_workers=NUM_THREADS_MARKETINDEX) as executor:
            executor.map(process_marketindex, retry_list)

    # Final data collection
    df_combined = pd.DataFrame(marketindex_results)

    columns = [
        "Date", "Ticker", "Company", "Sector", "Market Cap", "Shares Outstanding",
        "Open", "Close", "Volume", "Change (%)",
        "1 Month Return (%)", "3 Month Return (%)", "6 Month Return (%)",
        "12 Month Return (%)", "3 Year Return (%)", "5 Year Return (%)"
    ]

    # Save to CSV
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    df_combined.to_csv(f"{OUTPUT_FOLDER}/ASX_combined_final.csv", index=False, columns=columns)
    print(f"\n✅ Combined data saved to {OUTPUT_FOLDER}/ASX_combined_final.csv")

    if marketindex_failed:
        pd.DataFrame({"Ticker": marketindex_failed}).to_csv(f"{OUTPUT_FOLDER}/ASX_fully_failed.csv", index=False)
        print(f"⚠️ {len(marketindex_failed)} tickers failed. Saved to ASX_fully_failed.csv")
    else:
        print("✅ All tickers resolved!")

    # Upload to SQL
    try:
        engine = create_engine(
            'mysql+pymysql://sam:sam2025@database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/ASX_Market'
        )
        meta = MetaData()

        with engine.connect() as conn:
            if not engine.dialect.has_table(conn, "ASX_Data", schema="ASX_Market"):
                asx_table = Table(
                    'ASX_Data', meta,
                    Column('Date', String(20)),
                    Column('Ticker', String(10)),
                    Column('Company', String(255)),
                    Column('Sector', String(100)),
                    Column('Market Cap', Float),
                    Column('Shares Outstanding', Float),
                    Column('Open', Float),
                    Column('Close', Float),
                    Column('Volume', Float),
                    Column('Change (%)', Float),
                    Column('1 Month Return (%)', Float),
                    Column('3 Month Return (%)', Float),
                    Column('6 Month Return (%)', Float),
                    Column('12 Month Return (%)', Float),
                    Column('3 Year Return (%)', Float),
                    Column('5 Year Return (%)', Float),
                    schema='ASX_Market'
                )
                meta.create_all(engine)
                print("📋 SQL table 'ASX_Data' created in schema ASX_Market.")

        df_combined.to_sql(name='ASX_Data', con=engine, if_exists='append', index=False,  schema='ASX_Market')
        print("✅ Upload to SQL complete!")
    except Exception as e:
        print(f"❌ Failed to upload to SQL: {e}")


# === ENTRY POINT ===
if __name__ == "__main__":
    run_combined_pipeline()
