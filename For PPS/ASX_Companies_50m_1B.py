import pandas as pd
import cloudscraper
from concurrent.futures import ThreadPoolExecutor
import time
import os

# === CONFIG ===
APP_ID = "af5f4d73c1a54a33"  # Your app ID
NUM_THREADS = 10
MAX_RETRIES = 2
OUTPUT_FOLDER = r"C:\Users\HarryBox\Documents\Investair\For PPS\CSVOutput"  # <--- CHANGE THIS TO WHEREVER YOU WANT FILES SAVED

# === GLOBAL SESSION ===
session = cloudscraper.create_scraper()

# === SECTOR MAP SETUP ===
def get_sector_map():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    df = pd.read_csv(url, skiprows=1)
    possible_sector_cols = ["Industry Group", "GICS industry group", "Industry", "GICS Sector"]
    sector_col = next((col for col in possible_sector_cols if col in df.columns), None)
    df["Sector"] = df[sector_col] if sector_col else "Unknown"
    df["Ticker"] = df["ASX code"].str.strip().str.upper()
    return df.set_index("Ticker")["Sector"].to_dict()

SECTOR_MAP = get_sector_map()

# === FETCH MARKET DATA FUNCTION ===
def fetch_market_data(ticker):
    url = f"https://quoteapi.com/api/v5/symbols/{ticker.lower()}.asx"
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
        "referer": f"https://www.marketindex.com.au/asx/{ticker.lower()}"
    }
    try:
        response = session.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        quote = data.get("quote", {})
        fundamentals = data.get("fundamentals", {})
        desc = data.get("desc", {})

        close_price = quote.get("close") or quote.get("price")
        shares_issued = fundamentals.get("sharesIssued")

        if close_price and shares_issued:
            market_cap = float(close_price) * float(shares_issued)
        else:
            market_cap = None

        sector = SECTOR_MAP.get(ticker.upper(), "Unknown")

        return {
            "Ticker": ticker.upper(),
            "Company": desc.get("issuerName", "Unknown"),
            "Sector": sector,
            "Market Cap": market_cap
        }
    except Exception:
        return None

# === MAIN FUNCTION ===
def find_software_services_companies_in_range():
    # Load the full ASX company list
    df_listed = pd.read_csv("https://www.asx.com.au/asx/research/ASXListedCompanies.csv", skiprows=1)
    df_listed = df_listed[["ASX code", "Company name"]].dropna()
    tickers = df_listed["ASX code"].str.strip().str.upper().tolist()

    results = []
    failed = []

    def process(ticker):
        for attempt in range(MAX_RETRIES):
            result = fetch_market_data(ticker)
            if result:
                results.append(result)
                print(f"✅ {ticker}")
                return
            time.sleep(1 + attempt)
        failed.append(ticker)
        print(f"❌ Failed: {ticker}")

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(process, tickers)

    # Create a dataframe
    df = pd.DataFrame(results)

    # Filter companies:
    # - Market Cap between $100M and $1B
    # - Sector == "Software & Services"
    df_filtered = df[
        (df["Market Cap"] >= 50_000_000) &
        (df["Market Cap"] <= 1_000_000_000) &
        (df["Sector"] == "Software & Services")
    ]

    # === Save to OUTPUT_FOLDER ===
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    output_file = os.path.join(OUTPUT_FOLDER, "asx_software_services_100m_to_1b.csv")
    df_filtered.to_csv(output_file, index=False)

    print(f"\n✅ Filtered Software & Services companies saved to '{output_file}'")
    if failed:
        print(f"⚠️ {len(failed)} tickers failed and were skipped.")

if __name__ == "__main__":
    find_software_services_companies_in_range()
