# === IMPORTS ===
import os
import threading
import pandas as pd
import cloudscraper
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import time

# === CONFIG ===
OUTPUT_FOLDER = "Full ASX Snapshot"
APP_ID = "af5f4d73c1a54a33"
MAX_MARKETINDEX_RETRIES = 2
NUM_THREADS_MARKETINDEX = 10
DATE_USED = datetime.now(timezone(timedelta(hours=10))).date()

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
            "Info Date": DATE_USED,
            "Ticker": ticker.upper(),
            "Company": desc.get("issuerName", "Unknown"),
            "Sector": sector,
            "Market Cap": market_cap,
            "Shares Outstanding": shares_issued,
            "Open": open_price,
            "Close": close_price,
            "Volume": volume,
            "Change (%)": pct_change
        }
    except Exception:
        return None

# === MAIN RUN FUNCTION ===
def run_combined_pipeline():
    df_listed = pd.read_csv("https://www.asx.com.au/asx/research/ASXListedCompanies.csv", skiprows=1)
    df_listed = df_listed[["ASX code", "Company name"]].dropna()
    df_listed.columns = ["asx_code", "Company"]
    tickers = df_listed["asx_code"].str.strip().str.upper().tolist()

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

    # === Filter and Sort ===
    df_combined = pd.DataFrame(marketindex_results)

    if df_combined.empty:
        print("❌ No market data could be fetched. Exiting.")
        return

    if "Market Cap" not in df_combined.columns:
        print(f"❌ 'Market Cap' column missing. Columns found: {df_combined.columns.tolist()}")
        return

    df_combined = df_combined[df_combined["Market Cap"].notnull()]
    df_combined = df_combined.sort_values("Market Cap", ascending=False).iloc[300:]

    columns = [
        "Info Date", "Ticker", "Company", "Sector", "Market Cap", "Shares Outstanding",
        "Open", "Close", "Volume", "Change (%)"
    ]

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    df_combined.to_csv(f"{OUTPUT_FOLDER}/ASX_combined_final_OUtside300.csv", index=False, columns=columns)
    print(f"\n✅ Final data (excluding top 300 by market cap) saved to {OUTPUT_FOLDER}/ASX_combined_final.csv")

    if marketindex_failed:
        pd.DataFrame({"Ticker": marketindex_failed}).to_csv(f"{OUTPUT_FOLDER}/ASX_fully_failed_outside300.csv", index=False)
        print(f"⚠️ {len(marketindex_failed)} tickers failed. Saved to ASX_fully_failed.csv")
    else:
        print("✅ All tickers resolved!")


# === ENTRY POINT ===
if __name__ == "__main__":
    run_combined_pipeline()
