import pandas as pd
from sqlalchemy import create_engine


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
   
def get_files_sql(tickers_df: pd.DataFrame,
                          rep_type_codes: list[str] = ["02001"],
                          mysql_url: str = None) -> pd.DataFrame:
    """
    Given a DataFrame of tickers, return all filenames in ASX_RepTypes
    whose RepType Code is in rep_type_codes and whose Ticker is in that list.
    """
    # 1) Unique, uppercase tickers
    tickers = tickers_df["Ticker"].str.upper().unique().tolist()
    print(f"Looking up files for {len(tickers)} tickers…")

    # 2) Connect to MySQL
    if mysql_url is None:
        mysql_url = (
            "mysql+pymysql://sam:sam2025@"
            "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/ASX_Market"
        )
    engine = create_engine(mysql_url)

    # 3) Build our IN-clauses
    placeholders_rt = ",".join(["%s"] * len(rep_type_codes))
    placeholders_tk = ",".join(["%s"] * len(tickers))
    sql = f"""
      SELECT `Ticker`, `RepType Code`, `Filename`, `Date`
      FROM `ASX_Market`.`ASX_RepTypes`
      WHERE `RepType Code` IN ({placeholders_rt})
        AND `Ticker`       IN ({placeholders_tk})
    """

    # 4) Flatten params into one tuple
    params = tuple(rep_type_codes + tickers)

    # 5) Execute
    rep_df = pd.read_sql_query(sql, engine, params=params)
    print(f"  → Found {len(rep_df)} matching records.")

    # 6) Merge back so every ticker appears
    result = (
        tickers_df[["Ticker"]]
        .drop_duplicates()
        .merge(rep_df, on="Ticker", how="left")
        .reset_index(drop=True)
    )

    return result.dropna(subset=["Filename"]).reset_index(drop=True)

def spilt_filename(df):
    filename_col = df.columns[2]  # third column
    
    def split_fn(fn):
        if isinstance(fn, str) and len(fn) >= 16:
            return fn[:8], fn[8:16]
        else:
            return None, None

    splits = df[filename_col].apply(split_fn)
    df = df.copy()
    df["Folder"] = splits.apply(lambda x: x[0])
    df["File"] = splits.apply(lambda x: x[1])
    return df

def process_files(df, prompts, bucket, sql_columns):
    
    for idx, row in df.iterrows():
        file_type = str(row.get("RepType Code", "")).strip()
        
        Folder = row.get("Folder")
        Filename = row.get("File")
        Date = row.get("Date")
        
        Key = Folder + "/" + Filename + ".pdf"

        

        # Lookup inputs list for folder type; default empty list if not found
        input_list = prompts.get(file_type, [])
        


        # Build a single JSON-like dict
        data_json = {
            "bucket": bucket,
            "key": Key,
            "rep_type": file_type,
            "fields": input_list,
            "columns": sql_columns,
        }


def run_combined_pipeline():
    df = get_tickers_by_sector("Capital Goods")
    # Step 2: Get all matching announcement files
    files_df = get_files_sql(
        tickers_df=df,
        rep_type_codes=["02002", "02001", "02003"], #02001 = Becoming, 02002 = change 02003 = cease 
        mysql_url=None
    )
    
    matched = files_df.dropna(subset=["Filename"])
    
    split = spilt_filename(matched)
    
    
    prompts_lookup = {
        "02001": ["Substantial Holder name", "Number of Securities", "Voting Power"],
        "02002": ["Substantial Holder name", "Previous Number of Securities", "Previous Voting Power", "Present Number of securities", "Present Voting Power"],
        "02003": ["Substantial Holder name", "Previous Number of Securities", "Previous Voting Power"],
    }
    
    bucket = "xtf-asx"
    sql_columns = ["shareholder_name", "previous_votes","previous_power", "present_votes", "present_power" ]
    
    process_files(split, prompts_lookup, bucket, sql_columns)
    
    
    



if __name__ =="__main__":
    run_combined_pipeline()