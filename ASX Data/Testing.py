import cloudscraper
import requests
import pandas as pd
import time
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# === Config ===
APP_ID = "af5f4d73c1a54a33"
FAILED_INPUT_FILE = "Full ASX Snapshot Retry/ASX_failed_final.csv"
OUTPUT_FILE = "Full ASX Snapshot Retry/ASX_marketindex_fallback_cloudscraper.csv"
FAILED_OUTPUT_FILE = "Full ASX Snapshot Retry/ASX_marketindex_final_failed.csv"
MAX_THREADS = 10
RETRY_WAIT = 1.0
MAX_RETRIES = 3

# === Create scraper session
session = cloudscraper.create_scraper()

def get_sector_map():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    df = pd.read_csv(url, skiprows=1)

    # Print available columns for debugging if needed
    # print("Available columns:", df.columns.tolist())

    # Find a column that contains sector information
    possible_sector_cols = ["Industry Group", "GICS industry group", "Industry", "GICS Sector"]
    sector_col = next((col for col in possible_sector_cols if col in df.columns), None)

    if sector_col is None:
        print("⚠️ No sector column found in ASXListedCompanies.csv — defaulting sector to 'Unknown'")
        df["Sector"] = "Unknown"
    else:
        df["Sector"] = df[sector_col]

    df["Ticker"] = df["ASX code"].str.strip().str.upper()
    return df.set_index("Ticker")["Sector"].to_dict()

SECTOR_MAP = get_sector_map()

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
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/123.0.0.0 Safari/537.36",
        "referer": f"https://www.marketindex.com.au/asx/{ticker_lower}",
    }

    try:
        response = session.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        quote = data.get("quote", {})
        fundamentals = data.get("fundamentals", {})
        desc = data.get("desc", {})

        # Fallback to 'price' if open/close are None
        open_price = quote.get("open")
        close_price = quote.get("close")
        price_fallback = quote.get("price")

        if open_price is None:
            open_price = price_fallback
        if close_price is None:
            close_price = price_fallback

        volume = quote.get("volume")
        shares_issued = fundamentals.get("sharesIssued")
        market_cap = None

        if shares_issued is not None and close_price is not None:
            try:
                market_cap = round(float(shares_issued) * float(close_price), 2)
            except:
                market_cap = None

        pct_change = None
        if open_price and close_price:
            try:
                pct_change = round(((float(close_price) - float(open_price)) / float(open_price)) * 100, 2)
            except:
                pct_change = None

        sector = SECTOR_MAP.get(ticker.upper().replace(".AX", ""), "Unknown")
        date_today = datetime.now().strftime("%Y-%m-%d")

        return {
            "Date": date_today,
            "Ticker": ticker.upper().replace(".ASX", ""),
            "Company": desc.get("issuerName", "Unknown"),
            "Sector": sector,
            "Market Cap": market_cap,
            "Shares Outstanding": shares_issued,
            "Open": open_price,
            "Close": close_price,
            "Volume": volume,
            "Change (%)": pct_change
        }

    except Exception as e:
        return None

# === Retry wrapper
def fetch_with_retries(ticker, max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        result = fetch_marketindex_data(ticker)
        if result:
            return result
        time.sleep(RETRY_WAIT + random.random())
    return None

# === Main runner
def run_cloudscraper_marketindex():
    if not os.path.exists(FAILED_INPUT_FILE):
        print(f"❗ Missing failed tickers file: {FAILED_INPUT_FILE}")
        return

    failed_df = pd.read_csv(FAILED_INPUT_FILE)
    tickers = failed_df["Ticker"].dropna().unique().tolist()
    print(f"🌐 Scraping Market Index JSON for {len(tickers)} failed tickers using {MAX_THREADS} threads...")

    results = []
    failed_final = []

    def task(ticker):
        time.sleep(RETRY_WAIT * random.random())  # Staggered start
        return ticker, fetch_with_retries(ticker)

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_ticker = {executor.submit(task, ticker): ticker for ticker in tickers}

        for future in as_completed(future_to_ticker):
            ticker, result = future.result()
            if result:
                results.append(result)
                print(f"✅ {ticker} ✓")
            else:
                failed_final.append(ticker)
                print(f"❌ {ticker} ✗ (after {MAX_RETRIES} attempts)")

    # Save successful results with correct column order
    if results:
        df = pd.DataFrame(results)
        columns = ["Date", "Ticker", "Company", "Sector", "Market Cap", "Shares Outstanding", "Open", "Close","Volume", "Change (%)"]
        df.to_csv(OUTPUT_FILE, index=False, columns=columns)
        print(f"💾 Successfully fetched data saved to {OUTPUT_FILE}")
    else:
        print("⚠️ No successful data was fetched.")

    # Save failed tickers
    if failed_final:
        pd.DataFrame({"Ticker": failed_final}).to_csv(FAILED_OUTPUT_FILE, index=False)
        print(f"⚠️ {len(failed_final)} tickers failed after {MAX_RETRIES} retries. Saved to {FAILED_OUTPUT_FILE}")
    else:
        print("✅ All tickers were fetched successfully.")

# === Run it
if __name__ == "__main__":
    run_cloudscraper_marketindex()
