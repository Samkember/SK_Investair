import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
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
MAX_RETRIES = 2

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

# === Fetch Summary for One Ticker ===
def fetch_stock_summary(ticker, company_name=None):
    while True:
        while rate_limited_event.is_set():
            time.sleep(5)

        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            shares_outstanding = info.get("sharesOutstanding")

            hist = stock.history(period="13mo", interval="1d")
            hist.reset_index(inplace=True)
            hist = hist[hist["Volume"] > 0].dropna(subset=["Close"])

            yesterday = (datetime.now() - timedelta(days=1)).date()
            row = hist[hist["Date"].dt.date == yesterday]
            if row.empty:
                return None

            latest = row.iloc[0]
            close_yesterday = latest["Close"]
            volume_yesterday = latest["Volume"]
            open_yesterday = latest["Open"]
            pct_change_yesterday = round(((close_yesterday - open_yesterday) / open_yesterday) * 100, 2)
            market_cap = round(close_yesterday * shares_outstanding) if shares_outstanding else None

            def get_return(days):
                past_date = yesterday - timedelta(days=days)
                past_data = hist[hist["Date"].dt.date <= past_date]
                if not past_data.empty:
                    price_then = past_data.iloc[-1]["Close"]
                    return round(((close_yesterday - price_then) / price_then) * 100, 2)
                return None

            return {
                "Date": yesterday,
                "Ticker": ticker.replace(".AX", ""),
                "Company": info.get("longName") or company_name or "Unknown",
                "Sector": info.get("sector", "Unknown"),
                "Market Cap": market_cap,
                "Shares Outstanding": shares_outstanding,
                "Open": open_yesterday,
                "Close": close_yesterday,
                "Volume": volume_yesterday,
                "Change (%)": f"{pct_change_yesterday}%",
                "1 Month Return (%)": f"{get_return(30)}%",
                "3 Month Return (%)": f"{get_return(90)}%",
                "6 Month Return (%)": f"{get_return(180)}%",
                "12 Month Return (%)": f"{get_return(365)}%"
            }

        except Exception as e:
            msg = str(e)
            if "Too Many Requests" in msg or "rate limit" in msg.lower():
                with print_lock:
                    print(f"⚠️ Rate limit triggered by {ticker}. Pausing all threads for {RATE_LIMIT_WAIT_SECONDS}s.")
                rate_limited_event.set()
                time.sleep(RATE_LIMIT_WAIT_SECONDS)
                rate_limited_event.clear()
                continue
            else:
                with print_lock:
                    print(f"❌ Error with {ticker}: {e}")
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

# === Run One Pass of Tickers ===
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

# === Main Runner With Retries and Clean Exit ===
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

    # ✅ Final save of all successful summaries
    df_summary = pd.DataFrame(all_summaries)
    df_summary.to_csv("ASX_summary.csv", index=False)
    print("✅ All ASX summaries saved to ASX_summary.csv")

    # ✅ Save the full summary of successful tickers
    df_summary.to_csv(f"{LOG_FOLDER}/ASX_successful_final.csv", index=False)
    print(f"✅ {len(df_summary)} tickers saved to ASX_successful_final.csv with full data")

    # ✅ Save cleaned failed tickers list for scraping
    if not failed_df.empty:
        failed_tickers_clean = failed_df["ticker"].str.replace(".AX", "", regex=False)
        failed_tickers_clean.to_frame(name="Ticker").to_csv(f"{LOG_FOLDER}/ASX_failed_final.csv", index=False)
        print(f"⚠️ {len(failed_tickers_clean)} tickers failed after {MAX_RETRIES} retries. Saved to ASX_failed_final.csv (cleaned for scraping)")

    print(f"✅ Completed all {total_tickers} tickers. Final results written.")
    print("🏁 Exiting program cleanly.\n")
    sys.exit(0)






# === Run It ===
if __name__ == "__main__":
    fetch_all_asx_summaries_threaded()