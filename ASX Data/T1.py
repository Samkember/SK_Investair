import pandas as pd
import cloudscraper
from datetime import datetime
from dateutil.relativedelta import relativedelta

def fetch_quoteapi_ticks_all_returns(ticker: str, app_id: str = "af5f4d73c1a54a33"):
    scraper = cloudscraper.create_scraper()
    url = f"https://quoteapi.com/api/v5/symbols/{ticker.lower()}.asx/ticks"
    headers = {
        "accept": "application/json",
        "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "referer": f"https://www.marketindex.com.au/asx/{ticker.lower()}?src=search-all"
    }

    range_options = ["20y", "5y", "1y", "6m", "3m"]

    for range_ in range_options:
        try:
            params = {
                "appID": app_id,
                "adjustment": "capital",
                "fields": "dc",
                "range": range_
            }

            response = scraper.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            ticks = data.get("ticks", {})
            dates = ticks.get("date", [])
            closes = ticks.get("close", [])

            if not dates or not closes or len(dates) != len(closes):
                print(f"⚠️ No usable ticks found in range {range_}")
                continue

            print(f"✅ Using range: {range_}")

            df = pd.DataFrame({
                "Date": dates,
                "Price": closes
            })
            df["Date"] = pd.to_datetime(df["Date"])
            df.set_index("Date", inplace=True)
            df.sort_index(inplace=True)
            df = df[df["Price"].notnull()]  # Only keep rows with valid prices

            if df.empty:
                continue

            latest_price = df["Price"].iloc[-1]
            today = df.index[-1]

            offsets = {
                "1 Month Return (%)": today - relativedelta(months=1),
                "3 Month Return (%)": today - relativedelta(months=3),
                "6 Month Return (%)": today - relativedelta(months=6),
                "12 Month Return (%)": today - relativedelta(months=12),
                "3 Year Return (%)": today - relativedelta(years=3),
                "5 Year Return (%)": today - relativedelta(years=5),
            }

            returns = {}
            for label, target_date in offsets.items():
                valid_dates = df.index[df.index <= target_date]
                if not valid_dates.empty:
                    closest_date = valid_dates.max()
                    past_price = df.loc[closest_date, "Price"]
                    returns[label] = round(((latest_price - past_price) / past_price) * 100, 2)
                else:
                    returns[label] = None

            return returns

        except Exception as e:
            print(f"⚠️ Failed with range {range_}: {e}")
            try:
                print("Raw response content:", response.text)
            except:
                pass
            continue

    raise ValueError(f"❌ No historical price data found for {ticker.upper()} in any range.")



returns = fetch_quoteapi_ticks_all_returns("mom")
for label, value in returns.items():
    print(f"{label}: {value}%")