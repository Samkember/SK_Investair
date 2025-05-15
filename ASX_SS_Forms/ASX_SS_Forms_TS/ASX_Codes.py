## Gets Tickers from asx directly

## Examples Uses

# All companies
# all_co = get_tickers_by_sector()

# Just Capital Goods
# cg_co = get_tickers_by_sector("Capital Goods")

# Multiple sectors
# multi_co = get_tickers_by_sector([
#     "Energy",
#     "Materials",
#     "Health Care Equipment & Services"
# ])

import pandas as pd

def get_tickers_by_sector(sectors=None):
    """
    Fetch ASX tickers with their Company Name and Sector, then filter by sector.

    Args:
      sectors (str or list of str or None): 
        - None or "all" → return all sectors.
        - string        → return tickers in that one sector.
        - list[str]     → return tickers in any of those sectors.

    Returns:
      pd.DataFrame with columns ["Ticker", "CompanyName", "Sector"].
    """
    # 1) pull the master CSV
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    df = pd.read_csv(url, skiprows=1)

    # 2) detect the sector column
    possible = ["Industry Group", "GICS industry group", "Industry", "GICS Sector"]
    sector_col = next((c for c in possible if c in df.columns), None)
    df["Sector"] = df[sector_col].astype(str).str.strip() if sector_col else "Unknown"

    # 3) normalize tickers and extract company name
    df["Ticker"]      = df["ASX code"].astype(str).str.upper().str.strip()
    df["CompanyName"] = df["Company name"].astype(str).str.strip()

    # 4) interpret the sectors argument
    if sectors is None or (isinstance(sectors, str) and sectors.lower() == "all"):
        mask = pd.Series(True, index=df.index)
    else:
        wanted = {sectors} if isinstance(sectors, str) else set(sectors)
        mask = df["Sector"].isin(wanted)

    # 5) return only the three columns for the filtered rows
    return df.loc[mask, ["Ticker", "CompanyName", "Sector"]].reset_index(drop=True)
