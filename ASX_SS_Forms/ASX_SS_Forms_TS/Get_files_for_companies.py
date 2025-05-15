import pandas as pd
from sqlalchemy import create_engine


def get_files_for_tickers(tickers_df: pd.DataFrame,
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
      SELECT `Ticker`, `RepType Code`, `Filename`
      FROM `ASX_Market`.`ASX_RepTypes`
      WHERE `RepType Code` IN ({placeholders_rt})
        AND `Ticker`       IN ({placeholders_tk})
    """

    # 4) Flatten params into one tuple (not a list) so pandas does a single execution
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