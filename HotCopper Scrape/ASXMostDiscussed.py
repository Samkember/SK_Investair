import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import os

# === CONFIG ===
URL = "https://hotcopper.com.au/postview/"
SAVE_DIR = r"C:\Users\HarryBox\Documents\Investair\HotCopper Scrape"
WIDGET_CSV = os.path.join(SAVE_DIR, "md_widget_text.csv")

# === SETUP ===
os.makedirs(SAVE_DIR, exist_ok=True)
scraper = cloudscraper.create_scraper()
response = scraper.get(URL)
soup = BeautifulSoup(response.text, "html.parser")

# === SCRAPE md_widget ===
widget = soup.select_one("#md_widget")

if widget:
    # Extract and clean text
    widget_text = widget.get_text(separator="|", strip=True)
    widget_text = widget_text.replace("Most Discussed Stocks|(today)|View All|", "")
    parts = widget_text.split('|')

    # Parse stock data
    stock_data = []
    for i in range(0, len(parts) - 4, 5):
        ticker = parts[i].strip()
        company = parts[i+1].strip()
        comments = parts[i+4].strip()

        if ticker and company and comments.isdigit():
            stock_data.append({
                "Ticker": ticker,
                "Company": company,
                "Comments": int(comments)
            })

    # Create DataFrame
    df = pd.DataFrame(stock_data)

    # Save to CSV
    df.to_csv(WIDGET_CSV, index=False)
    print(f"\n✅ Most Discussed Stocks saved to: {WIDGET_CSV}")

    # === GRAPHICAL OUTPUT: Horizontal Bar Chart ===
    df_sorted = df.sort_values("Comments", ascending=True)

    plt.figure(figsize=(10, 8))
    plt.barh(df_sorted["Ticker"], df_sorted["Comments"])
    plt.title("Most Discussed ASX Stocks Today", fontsize=16)
    plt.xlabel("Number of Comments", fontsize=12)
    plt.ylabel("Ticker", fontsize=12)

    # Annotate each bar with comment numbers
    for index, value in enumerate(df_sorted["Comments"]):
        plt.text(value, index, str(value), va='center', fontsize=8)

    plt.tight_layout()
    plt.show()

else:
    print("❌ md_widget not found on the page.")
