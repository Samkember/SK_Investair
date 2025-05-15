import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd

def extract_shareholder_tables(ticker):
    url = f"https://www.marketindex.com.au/asx/{ticker.lower()}"
    scraper = cloudscraper.create_scraper()
    
    try:
        response = scraper.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        def extract_table(heading_text, columns):
            heading = soup.find(["h2", "h3"], string=lambda s: s and heading_text.lower() in s.lower())
            if not heading:
                return None
            table = heading.find_next("table", class_="mi-table")
            if not table:
                return None
            rows = table.find("tbody").find_all("tr")
            data = []
            for row in rows:
                cells = [td.get_text(strip=True).replace(",", "") for td in row.find_all("td")]
                if len(cells) == len(columns):
                    data.append(dict(zip(columns, cells)))
            return pd.DataFrame(data)

        return {
            "Substantial Shareholders": extract_table(
                "Substantial Shareholders",
                ["Name", "Last Notice", "Total Shares", "Shares Held (%)"]
            ),
            "Shareholders Buying": extract_table(
                "Shareholders Buying",
                ["Date", "Name", "Bought", "Previous %", "New %"]
            ),
            "Shareholders Selling": extract_table(
                "Shareholders Selling",
                ["Date", "Name", "Sold", "Previous %", "New %"]
            )
        }

    except Exception as e:
        print(f"❌ Failed to fetch shareholder data for {ticker}: {e}")
        return {}

# === RUN ===
data = extract_shareholder_tables("A4N")

for section, df in data.items():
    print(f"\n📊 {section}:\n")
    print(df if df is not None else "No data found.")
