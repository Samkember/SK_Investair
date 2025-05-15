import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import random

# === CONFIG ===
APP_ID = "af5f4d73c1a54a33"
MAX_THREADS = 1
MAX_RETRIES = 7
BASE_DELAY = 2

# === SESSIONS ===
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
)
session = cloudscraper.create_scraper()

# === GLOBAL LOCKS + STORAGE ===
lock_sub = threading.Lock()
lock_buy = threading.Lock()
lock_sell = threading.Lock()
substantial_all, buying_all, selling_all = [], [], []

# === GET ALL TICKERS FROM ASX CSV ===
def get_all_tickers():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    df = pd.read_csv(url, skiprows=1)
    possible_sector_cols = ["Industry Group", "GICS industry group", "Industry", "GICS Sector"]
    sector_col = next((col for col in possible_sector_cols if col in df.columns), None)
    df["Sector"] = df[sector_col] if sector_col else "Unknown"
    df["Ticker"] = df["ASX code"].str.strip().str.upper()
    df["Company"] = df["Company name"]

    print(df)
    return df[["Ticker", "Company", "Sector"]]

# === TEST IF TICKER EXISTS VIA OPEN PRICE ===
def get_open_price(ticker):
    url = f"https://quoteapi.com/api/v5/symbols/{ticker.lower()}.asx"
    headers = {
        "accept": "application/json",
        "referer": f"https://www.marketindex.com.au/asx/{ticker.lower()}",
        "user-agent": "Mozilla/5.0"
    }
    params = {
        "appID": APP_ID,
        "liveness": "overnight"
    }
    try:
        response = session.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        quote = data.get("quote", {})
        return quote.get("open") or quote.get("price")
    except Exception:
        return None

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
                return  # If the substantial shareholders table is missing, fail the ticker

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
    df_all = get_all_tickers()
    valid_tickers = []

    print("🔍 Checking tickers for valid open price...")

    for _, row in df_all.iterrows():
        ticker = row["Ticker"]
        if get_open_price(ticker) is not None:
            valid_tickers.append(ticker)
            print(f"✅ {ticker} valid")
        else:
            print(f"❌ {ticker} invalid")

    print(f"\n🎯 {len(valid_tickers)} tickers passed open price check. Beginning shareholder scraping...\n")

    start = time.time()

    def batch_scraper(batch):
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            executor.map(extract_shareholder_tables, batch)

    for i in range(0, len(valid_tickers), 50):  # Update the step to 50
        batch = valid_tickers[i:i+50]  # Batch of 50 tickers
        batch_scraper(batch)

        # Save partial results every 50 tickers
        if substantial_all:
            pd.concat(substantial_all, ignore_index=True).to_csv(f"checkpoint_substantial_{i+50}.csv", index=False)
        if buying_all:
            pd.concat(buying_all, ignore_index=True).to_csv(f"checkpoint_buying_{i+50}.csv", index=False)
        if selling_all:
            pd.concat(selling_all, ignore_index=True).to_csv(f"checkpoint_selling_{i+50}.csv", index=False)

        print(f"💾 Checkpoint saved after {i+50} tickers.")

    # Final save
    pd.concat(substantial_all, ignore_index=True).to_csv("all_substantial_shareholders.csv", index=False)
    pd.concat(buying_all, ignore_index=True).to_csv("all_shareholders_buying.csv", index=False)
    pd.concat(selling_all, ignore_index=True).to_csv("all_shareholders_selling.csv", index=False)

    print(f"\n✅ Done in {round(time.time() - start, 2)} seconds.")
