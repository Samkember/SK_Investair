import pandas as pd
from sqlalchemy import create_engine, text

# === GET ALL CAPITAL-GOODS TICKERS ===
def get_all_tickers():
    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
    df = pd.read_csv(url, skiprows=1)
    
    # Find whichever “sector” column is present
    possible_sector_cols = ["Industry Group", "GICS industry group", "Industry", "GICS Sector"]
    sector_col = next((c for c in possible_sector_cols if c in df.columns), None)
    
    if sector_col:
        df["Sector"] = df[sector_col].str.strip()
    else:
        df["Sector"] = "Unknown"
    
    df["Ticker"]  = df["ASX code"].str.strip().str.upper()
    df["Company"] = df["Company name"]
    
    # Keep only Capital Goods
    return df.loc[df["Sector"] == "Capital Goods", ["Ticker", "Company", "Sector"]]


def main():
    # 1) load tickers
    capgoods = get_all_tickers()
    tickers = capgoods["Ticker"].tolist()
    print(f"Found {len(tickers)} Capital Goods tickers")

    # 2) connect to MySQL (adjust user/pass/host/db as needed)
    engine = create_engine(
        "mysql+pymysql://sam:sam2025@"
        "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/ASX_Market"
    )

    # 3) pull the entire ASX_RepTypes table
    rep_df = pd.read_sql_table(
        table_name="ASX_RepTypes",
        con=engine,
        schema="ASX_Market"
    )
    print(f"Loaded {len(rep_df)} rows from ASX_RepTypes")

    # 4) filter for RepType Code '03002' and Ticker in our list
    mask = (
        (rep_df["RepType Code"].isin(["02001"])) & #, "02002", "02003"
        (rep_df["Ticker"].isin(tickers))
    )
    matches = rep_df.loc[mask, "Filename"].tolist()

    # count how many filenames matched
    match_count = len(matches)

    # 5) output results
    if match_count:
        print(f"\n✅ Found {match_count} files matching RepTypeCode=03002 & Capital Goods tickers:")
        for fn in matches:
            print("  ", fn)
    else:
        print("\nℹ️  No files found matching those criteria.")

if __name__ == "__main__":
    main()
