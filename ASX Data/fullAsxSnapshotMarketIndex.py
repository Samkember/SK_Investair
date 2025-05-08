import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os
import time
import sys

# === Global Settings ===
NUM_THREADS = 5
RATE_LIMIT_WAIT_SECONDS = 30
LOG_FOLDER = "Full ASX Snapshot Retry"
LOG_BATCH_SIZE = 100
MAX_RETRIES = 3

# === Globals and Locks ===
print_lock = threading.Lock()
result_lock = threading.Lock()
rate_limited_event = threading.Event()
log_counter = 0
snapshot_counter = 1
failed_tickers = []

# === Get ASX List ===
def get_asx_list():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    df = pd.read_csv(url, skiprows=1)
    df = df[["ASX code", "Company name"]].dropna()
    df.columns = ["asx_code", "Company"]
    df["ticker"] = df["asx_code"].str.strip().str.upper() + ".AX"
    return df[["ticker", "Company"]]

# === Fetch Stock Summary from Market Index ===
def fetch_stock_summary(ticker, company_name=None):
    try:
        asx_code = ticker.replace(".AX", "")
        url = f"https://www.marketindex.com.au/asx/{asx_code.lower()}"
        headers = {"User-Agent": "Mozilla/5.0"}

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            with print_lock:
                print(f"❌ Failed to fetch {ticker}. Status code: {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Get price
        price_tag = soup.find("span", {"class": "price"})
        price = float(price_tag.text.strip().replace("$", "")) if price_tag else None

        # Get sector
        sector_tag = soup.find("div", class_="sector")
        sector = sector_tag.text.strip() if sector_tag else "Unknown"

        # Market cap and volume
        metrics = soup.find_all("div", class_="snapshot__data-item")
        market_cap = volume = None
        for item in metrics:
            label = item.find("div", class_="snapshot__data-label")
            value = item.find("div", class_="snapshot__data-value")
            if not label or not value:
                continue
            label_text = label.text.strip()
            value_text = value.text.strip().replace(",", "").replace("$", "")
            if "Market Cap" in label_text:
                market_cap = value_text
            elif "Volume" in label_text:
                volume = value_text

        return {
            "Date": datetime.now().date(),
            "Ticker": asx_code,
            "Company": company_name or "Unknown",
            "Sector": sector,
            "Market Cap": market_cap,
            "Shares Outstanding": None,  # Not available
            "Open": None,
            "Close": price,
            "Volume": volume,
            "Change (%)": None,
            "1 Month Return (%)": None,
            "3 Month Return (%)": None,
            "6 Month Return (%)": None,
            "12 Month Return (%)": None
        }

    except Exception as e:
        with print_lock:
            print(f"❌ Error fetching {ticker}: {e}")
        return None

# === Thread Worker ===
def process_ticker(row, failed_list):
    ticker = row["ticker"]
    company_name = row["Company"]
    summary = fetch_stock_summary(ticker, company_name)
    if summary:
        with print_lock:
            print(f"✅ Successfully fetched {ticker}")
    else:
        with result_lock:
            failed_list.append(row)
    return summary

# === Batch Save Partial Snapshots ===
def save_snapshot(summaries, count):
    filename = f"{LOG_FOLDER}/ASX_snapshot_partial_{count}.csv"
    pd.DataFrame(summaries).to_csv(filename, index=False)
    with print_lock:
        print(f"💾 Saved snapshot of {len(summaries)} tickers to {filename}")

# === Run One Batch Pass ===
def run_batch(asx_rows, summaries, total_tickers):
    global log_counter, snapshot_counter
    batch_failed = []

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {
            executor.submit(process_ticker, row, batch_failed): row["ticker"]
            for _, row in asx_rows.iterrows()
        }

        for future in as_completed(futures):
            result = future.result()
            if result:
                with result_lock:
                    summaries.append(result)
                    log_counter += 1

                    if log_counter % LOG_BATCH_SIZE == 0:
                        save_snapshot(summaries, snapshot_counter)
                        snapshot_counter += 1

                    if log_counter % 100 == 0:
                        remaining = total_tickers - log_counter
                        with print_lock:
                            print(f"🧮 Progress: {log_counter} tickers processed, {remaining} remaining...")

    return pd.DataFrame(batch_failed)

# === Main Runner ===
def fetch_all_asx_summaries_threaded():
    if not os.path.exists(LOG_FOLDER):
        os.makedirs(LOG_FOLDER)

    base_df = get_asx_list()
    total_tickers = len(base_df)
    all_summaries = []
    failed_df = run_batch(base_df, all_summaries, total_tickers)

    retry_round = 1
    while not failed_df.empty and retry_round <= MAX_RETRIES:
        with print_lock:
            print(f"🔁 Starting retry round {retry_round} with {len(failed_df)} tickers...")
        time.sleep(3)
        failed_df = run_batch(failed_df, all_summaries, total_tickers)
        retry_round += 1

    # Final Save
    df_summary = pd.DataFrame(all_summaries)
    df_summary.to_csv("ASX_summary.csv", index=False)
    print("✅ All ASX summaries saved to ASX_summary.csv")

    if not failed_df.empty:
        failed_df.to_csv(f"{LOG_FOLDER}/ASX_failed_final.csv", index=False)
        print(f"⚠️ {len(failed_df)} tickers failed after {MAX_RETRIES} retries. Saved to ASX_failed_final.csv")

    print(f"✅ Completed all {total_tickers} tickers. Final results written.")
    print("🏁 Exiting program cleanly.\n")
    sys.exit(0)

# === Run It ===
if __name__ == "__main__":
    fetch_all_asx_summaries_threaded()

