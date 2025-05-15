import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def fetch_macquarie_info():
    ticker = "MQG.AX"
    base_code = "MQG"

    stock = yf.Ticker(ticker)
    company_info = stock.info
    shares_outstanding = company_info.get("sharesOutstanding")

    hist = stock.history(period="13mo", interval="1d")
    hist.reset_index(inplace=True)
    hist = hist[hist["Volume"] > 0].dropna(subset=["Close"])

    # Use yesterday's date (naive)
    yesterday = (datetime.now() - timedelta(days=1)).date()
    row = hist[hist["Date"].dt.date == yesterday]

    if row.empty:
        print(f"No data available for {yesterday}")
        return None

    latest = row.iloc[0]
    close_yesterday = round(latest["Close"], 2)
    volume_yesterday = latest["Volume"]
    open_yesterday = round(latest["Open"], 2)
    pct_change_yesterday = round(((close_yesterday - open_yesterday) / open_yesterday) * 100, 2)

    # Recalculate market cap
    market_cap_yesterday = (
        round(close_yesterday * shares_outstanding) if shares_outstanding else None
    )

    # Return calculator
    def get_return(days):
        past_date = yesterday - timedelta(days=days)
        past_data = hist[hist["Date"].dt.date <= past_date]
        if not past_data.empty:
            price_then = past_data.iloc[-1]["Close"]
            return round(((close_yesterday - price_then) / price_then) * 100, 2)
        return None

    summary = {
        "Date": yesterday,
        "Ticker": base_code,
        "Company": company_info.get("longName", "Unknown"),
        "Sector": company_info.get("sector", "Unknown"),
        "Market Cap": market_cap_yesterday,
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

    df = pd.DataFrame([summary])
    df.to_csv(f"{base_code}_summary.csv", index=False)
    print(f"Saved to {base_code}_summary.csv")
    return df

# Run and view
df_result = fetch_macquarie_info()
if df_result is not None:
    print(df_result.T)