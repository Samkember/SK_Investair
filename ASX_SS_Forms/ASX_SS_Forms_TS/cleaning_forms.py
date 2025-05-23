import pandas as pd
from sqlalchemy import create_engine
from rapidfuzz import process, fuzz
import re, sys

from ASX_Codes import get_tickers_by_sector

outputFile = r"C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms\output.csv"

# # Connect to MySQL
# mysql_url = (
#     "mysql+pymysql://sam:sam2025@"
#     "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/Substantial_Holding"
# )
# engine = create_engine(mysql_url)

# # === STEP 1: Load Table ===
# TABLE_NAME_new = "new"  # <-- replace with your actual table name
# TABLE_NAME_change = 'change_in'
# TABLE_NAME_cease = "cease"

# df_new = pd.read_sql(f"SELECT * FROM {TABLE_NAME_new} ORDER BY TICKER", con=engine)
# df_change = pd.read_sql(f"SELECT * FROM {TABLE_NAME_change} ORDER BY TICKER", con=engine)
# df_cease = pd.read_sql(f"SELECT * FROM {TABLE_NAME_cease} ORDER BY TICKER", con=engine)

# # Combine them all
# df_all = pd.concat([df_new, df_change, df_cease], ignore_index=True)

# # Export to CSV
# df_all.to_csv(outputFile, index=False)


import pandas as pd
from rapidfuzz import fuzz
from sqlalchemy import create_engine

SIMILARITY_THRESHOLD = 90  # Fuzzy match threshold

def cluster_names(df_all, similarity_threshold=90):
    canonical_map = {}
    for ticker in df_all["ticker"].dropna().unique():
        names = df_all[df_all["ticker"] == ticker]["shareholder_name"].dropna().unique()
        clusters = []
        for name in names:
            matched = False
            for cluster in clusters:
                if fuzz.token_sort_ratio(name, cluster[0]) >= similarity_threshold:
                    cluster.append(name)
                    matched = True
                    break
            if not matched:
                clusters.append([name])
        for cluster in clusters:
            canonical = cluster[0]
            for name in cluster:
                canonical_map[name] = canonical
    return canonical_map

def combine_all_events(df_new, df_change, df_cease, canonical_map):
    for df, label in zip([df_new, df_change, df_cease], ["new", "change", "cease"]):
        df["event_type"] = label
        df["shareholder_name"] = df["shareholder_name"].map(lambda x: canonical_map.get(x, x))
        df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df_all = pd.concat([df_new, df_change, df_cease], ignore_index=True)
    df_all = df_all.dropna(subset=["event_date"])
    df_all = df_all.sort_values(by=["ticker", "shareholder_name", "event_date"])
    return df_all

def get_shareholder_history(df_all):
    return df_all.groupby(["ticker", "shareholder_name"], group_keys=False).apply(
        lambda g: g.sort_values("event_date")
    ).reset_index(drop=True)

def get_latest_substantial_holders(df_all):
    df_active = df_all[df_all["event_type"] != "cease"]
    return df_active.sort_values("event_date").groupby(
        ["ticker", "shareholder_name"], group_keys=False
    ).tail(1).reset_index(drop=True)

def main():
    # Connect to MySQL
    mysql_url = (
        "mysql+pymysql://sam:sam2025@"
        "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/Substantial_Holding"
    )
    engine = create_engine(mysql_url)

    # === STEP 1: Load Table ===
    TABLE_NAME_new = "new"  # <-- replace with your actual table name
    TABLE_NAME_change = 'change_in'
    TABLE_NAME_cease = "cease"

    # Load data
    df_new = pd.read_sql(f"SELECT * FROM {TABLE_NAME_new} ORDER BY TICKER", con=engine)
    df_change = pd.read_sql(f"SELECT * FROM {TABLE_NAME_change} ORDER BY TICKER", con=engine)
    df_cease = pd.read_sql(f"SELECT * FROM {TABLE_NAME_cease} ORDER BY TICKER", con=engine)

    df_all_combined = pd.concat([df_new, df_change, df_cease], ignore_index=True)
    canonical_map = cluster_names(df_all_combined)

    df_all = combine_all_events(df_new, df_change, df_cease, canonical_map)

    history_df = get_shareholder_history(df_all)
    latest_df = get_latest_substantial_holders(df_all)

    history_df.to_csv("shareholder_history.csv", index=False)
    latest_df.to_csv("latest_shareholders.csv", index=False)

    print("✅ Exported shareholder_history.csv and latest_shareholders.csv")

if __name__ == "__main__":
    main()