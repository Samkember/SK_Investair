import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import random
import os

# === CONFIG ===
APP_ID = "af5f4d73c1a54a33"
MAX_THREADS = 1
MAX_RETRIES = 7
BASE_DELAY = 2
OUTPUT_FOLDER = r"C:\Users\HarryBox\Documents\Investair\For PPS\CSVOutput"  # Must match your earlier folder

# === LOAD ONLY TARGET TICKERS ===
def get_target_tickers():
    file_path = os.path.join(OUTPUT_FOLDER, "asx_software_services_100m_to_1b.csv")
    df = pd.read_csv(file_path)
    return df["Ticker"].dropna().unique().tolist()

# === GLOBAL LOCKS + STORAGE ===
scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False})
session = cloudscraper.create_scraper()

lock_sub = threading.Lock()
lock_buy = threading.Lock()
lock_sell = threading.Lock()
substantial_all, buying_all, selling_all = [], [], []

# === SHAREHOLDER SCRAPER ===
def extract_shareholder_tables(ticker, max_retries=MAX_RETRIES, base_delay=BASE_DELAY):
    url = f"https://www.marketindex.com.au/asx/{ticker.lower()}"
    attempt = 0

    while attempt < max_retries:
        try:
            response = scraper.get(url, timeout=10)

            if response.status_code == 429:
                if attempt < max_retries - 1:
                    wait = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(f"🚦 {ticker} rate limited (429). Waiting {round(wait, 2)}s...")
                    time.sleep(wait)
                    attempt += 1
                    continue
                else:
                    print(f"❌ {ticker} failed after {max_retries} attempts due to persistent 429.")
                    return

            if response.status_code == 403:
                print(f"🚫 {ticker} - Forbidden (403). Skipping.")
                return

            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            def extract_table(heading, columns):
                h = soup.find(["h2", "h3"], string=lambda s: s and heading.lower() in s.lower())
                if not h:
                    return None
                table = h.find_next("table", class_="mi-table")
                if not table:
                    return None
                rows = table.find("tbody").find_all("tr")
                data = []
                for row in rows:
                    cells = [td.get_text(strip=True).replace(",", "") for td in row.find_all("td")]
                    if len(cells) == len(columns):
                        entry = dict(zip(columns, cells))
                        entry["Ticker"] = ticker
                        data.append(entry)
                return pd.DataFrame(data)

            sub = extract_table("Substantial Shareholders", ["Name", "Last Notice", "Total Shares", "Shares Held (%)"])
            buy = extract_table("Shareholders Buying", ["Date", "Name", "Bought", "Previous %", "New %"])
            sell = extract_table("Shareholders Selling", ["Date", "Name", "Sold", "Previous %", "New %"])

            if sub is not None:
                with lock_sub:
                    substantial_all.append(sub)
            else:
                print(f"❌ {ticker} failed due to missing Substantial Shareholders table.")
                return

            if buy is not None:
                with lock_buy:
                    buying_all.append(buy)
            if sell is not None:
                with lock_sell:
                    selling_all.append(sell)

            print(f"✅ {ticker}")
            return

        except Exception:
            wait = base_delay * (2 ** attempt) + random.uniform(0, 1)
            attempt += 1

    print(f"❌ {ticker} failed after {max_retries} attempts")

# === MAIN EXECUTION ===
if __name__ == "__main__":
    # Load ONLY the filtered tickers
    target_tickers = get_target_tickers()
    print(f"🎯 {len(target_tickers)} companies to scrape based on market cap and sector filters.\n")

    start = time.time()

    def batch_scraper(batch):
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            executor.map(extract_shareholder_tables, batch)

    for i in range(0, len(target_tickers), 25):  # Smaller batches for reliability
        batch = target_tickers[i:i+25]
        batch_scraper(batch)

        # Save partial results every 25 tickers
        if substantial_all:
            pd.concat(substantial_all, ignore_index=True).to_csv(os.path.join(OUTPUT_FOLDER, f"checkpoint_substantial_{i+25}.csv"), index=False)
        if buying_all:
            pd.concat(buying_all, ignore_index=True).to_csv(os.path.join(OUTPUT_FOLDER, f"checkpoint_buying_{i+25}.csv"), index=False)
        if selling_all:
            pd.concat(selling_all, ignore_index=True).to_csv(os.path.join(OUTPUT_FOLDER, f"checkpoint_selling_{i+25}.csv"), index=False)

        print(f"💾 Checkpoint saved after {i+25} tickers.")

    # Final save
    pd.concat(substantial_all, ignore_index=True).to_csv(os.path.join(OUTPUT_FOLDER, "software_services_substantial_shareholders.csv"), index=False)
    pd.concat(buying_all, ignore_index=True).to_csv(os.path.join(OUTPUT_FOLDER, "software_services_shareholders_buying.csv"), index=False)
    pd.concat(selling_all, ignore_index=True).to_csv(os.path.join(OUTPUT_FOLDER, "software_services_shareholders_selling.csv"), index=False)

    print(f"\n✅ Done in {round(time.time() - start, 2)} seconds.")
