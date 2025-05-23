import pandas as pd
from sqlalchemy import create_engine
from rapidfuzz import process, fuzz
import re, sys
import numpy as np

from ASX_Codes import get_tickers_by_sector

outputFolder = r"C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms"
InputFolder = r"C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms\SS_CapitalGoods.csv"

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

# # Load data
# df_new = pd.read_sql(f"SELECT * FROM {TABLE_NAME_new} ORDER BY TICKER", con=engine)
# df_change = pd.read_sql(f"SELECT * FROM {TABLE_NAME_change} ORDER BY TICKER", con=engine)
# df_cease = pd.read_sql(f"SELECT * FROM {TABLE_NAME_cease} ORDER BY TICKER", con=engine)

# # Add source column (optional)
# df_new["source"] = "new"
# df_change["source"] = "change"
# df_cease["source"] = "cease"

# df_new.to_csv(rf"{outputFolder}\output_new.csv", index=False)
# df_change.to_csv(rf"{outputFolder}\output_change.csv", index=False)
# df_cease.to_csv(rf"{outputFolder}\output_cease.csv", index=False)


# df_tickers = (get_tickers_by_sector("Capital Goods")).iloc[:,0]

# pd.set_option("display.max_colwidth", None)


# SIMILARITY_THRESHOLD = 80
# VOTE_TOLERANCE = 0.05
# POWER_TOLERANCE = 0.2

# output_rows = []

# df_tickers = pd.concat([df_new["ticker"], df_change["ticker"], df_cease["ticker"]]).dropna().unique()

# for ticker in df_tickers:
#     df_n = df_new[df_new["ticker"] == ticker]
#     df_c = df_change[df_change["ticker"] == ticker]
#     df_s = df_cease[df_cease["ticker"] == ticker]

#     all_records = pd.concat([df_n, df_c, df_s], ignore_index=True)
#     all_records = all_records.dropna(subset=["shareholder_name"])

#     shareholder_groups = []

#     for _, row in all_records.iterrows():
#         name = row["shareholder_name"]
#         matched = False

#         for group in shareholder_groups:
#             rep_name, records = group["rep_name"], group["records"]
#             score = fuzz.token_sort_ratio(name, rep_name)

#             if score >= SIMILARITY_THRESHOLD:
#                 ref_row = records[0]

#                 try:
#                     current_votes = float(row.get("previous_votes") or row.get("present_votes") or 0)
#                     ref_votes = float(ref_row.get("previous_votes") or ref_row.get("present_votes") or 0)
#                     votes_match = abs(current_votes - ref_votes) <= VOTE_TOLERANCE * ref_votes

#                     current_power = float(row.get("previous_power") or row.get("present_power") or 0)
#                     ref_power = float(ref_row.get("previous_power") or ref_row.get("present_power") or 0)
#                     power_match = abs(current_power - ref_power) <= POWER_TOLERANCE
#                 except:
#                     votes_match = power_match = False

#                 if votes_match or power_match:
#                     group["records"].append(row)
#                     matched = True
#                     break

#         if not matched:
#             shareholder_groups.append({
#                 "rep_name": name,
#                 "records": [row]
#             })

#     for group in shareholder_groups:
#         df_group = pd.DataFrame(group["records"])
#         df_group["event_date"] = pd.to_datetime(df_group["event_date"], errors="coerce")
#         latest_record = df_group.sort_values("event_date").dropna(subset=["event_date"]).iloc[-1]

#         latest_record = latest_record.copy()
#         latest_record["ticker"] = ticker
#         latest_record["matched_group"] = group["rep_name"]
#         output_rows.append(latest_record)

# # Convert to DataFrame and save to CSV
# output_df = pd.DataFrame(output_rows)
# output_df.to_csv("output_latest_shareholders.csv", index=False)
# print("Saved latest shareholder records to 'output_latest_shareholders.csv'")




df = pd.read_csv(InputFolder)

print(df)

df_sorted = df.sort_values(by="ticker", ascending=True)




# for ticker in df_sorted["ticker"].unique():
#     for index, row in df_sorted.iterrows():
#         if row["ticker"] == ticker:
#             print(row)
            
#     sys.exit(0)
    


SIMILARITY_THRESHOLD = 70  # adjust as needed

# Ensure event_date is datetime
df_sorted["event_date"] = pd.to_datetime(df_sorted["event_date"], dayfirst=True, errors="coerce")

latest_records = []
no_info_tickers = []

for ticker in df_sorted["ticker"].unique():
    print(f"\n=== Ticker: {ticker} ===")
    rows = []

    for index, row in df_sorted.iterrows():
        if row["ticker"] == ticker:
            rows.append(row)

    matched_groups = []
    for row in rows:
        name = row["shareholder_name"]
        matched = False

        for group in matched_groups:
            score = fuzz.token_sort_ratio(name, group["rep_name"])
            if score >= SIMILARITY_THRESHOLD:
                group["records"].append(row)
                matched = True
                break

        if not matched:
            matched_groups.append({
                "rep_name": name,
                "records": [row]
            })

    if matched_groups:
        for group in matched_groups:
            records_df = pd.DataFrame(group["records"])
            records_df = records_df.dropna(subset=["event_date"])
            if not records_df.empty:
                latest_record = records_df.loc[records_df["event_date"].idxmax()]
                latest_records.append(latest_record)
            else:
                no_info_tickers.append(ticker)
    else:
        no_info_tickers.append(ticker)

# Combine to DataFrame
output_df = pd.DataFrame(latest_records)

# Add new columns based on conditions
output_df["Latest_Holdings"] = np.where(
    output_df["source"].isin(["new", "change"]),
    output_df["present_votes"],
    np.nan
)

output_df["Latest_Percentage"] = np.where(
    output_df["source"].isin(["new", "change"]),
    output_df["present_power"],
    np.nan
)

# Export final result
output_df.to_csv("latest_shareholder_info.csv", index=False)
print("Saved 'latest_shareholder_info.csv'")

# Save tickers with no valid info
pd.Series(no_info_tickers).drop_duplicates().to_csv("tickers_with_no_shareholder_info.csv", index=False)
print("Saved tickers with no shareholder info to 'tickers_with_no_shareholder_info.csv'")