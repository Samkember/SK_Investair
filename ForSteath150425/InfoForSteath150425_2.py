import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# === CONFIG ===
capital_goods_path = r"C:\Users\HarryBox\Documents\Full ASX Snapshot\Capital_Goods_Only.csv"
substantial_path = r"C:\Users\HarryBox\Documents\all_substantial_shareholders.csv"
buyers_path = r"C:\Users\HarryBox\Documents\all_shareholders_buying.csv"
sellers_path = r"C:\Users\HarryBox\Documents\all_shareholders_selling.csv"
excel_path = "Capital_Goods_Holdings_Complete.xlsx"
keywords = ["fund", "management", "asset", "partners", "capital", "advisors", "ventures", "trading", "investment", "holdings"]
pattern = '|'.join(keywords)

# === LOAD DATA ===
df_companies = pd.read_csv(capital_goods_path)
df_substantial = pd.read_csv(substantial_path)

# === CLEAN & NORMALIZE ===
df_companies.columns = df_companies.columns.str.strip()
df_substantial.columns = df_substantial.columns.str.strip()
df_companies["Ticker"] = df_companies["Ticker"].str.strip().str.upper()
df_substantial["Ticker"] = df_substantial["Ticker"].str.strip().str.upper()

# === FILTER COMPANIES BY MARKET CAP (80M–500M) ===
df_companies = df_companies[
    (df_companies["Market Cap"] >= 80_000_000) & 
    (df_companies["Market Cap"] <= 500_000_000)
]
df_companies.to_csv("Filtered_Capital_Goods_80M_500M.csv", index=False)

# === FIND SHARES HELD COLUMN ===
shares_col = next((col for col in df_substantial.columns if "share" in col.lower()), None)
if not shares_col:
    raise ValueError("❌ Could not find a 'Shares Held' column.")

# === MERGE WITH PRICE DATA ===
df_merged = df_substantial.merge(df_companies[["Ticker", "Close"]], on="Ticker", how="inner")
df_merged[shares_col] = df_merged[shares_col].astype(str).str.replace(",", "").astype(float)
df_merged["Close"] = pd.to_numeric(df_merged["Close"], errors="coerce")
df_merged = df_merged[df_merged["Close"] >= 0.01]

# === CALCULATE $ VALUE ===
df_merged["Holding Value ($)"] = df_merged[shares_col] * df_merged["Close"]

# === DETAILED & AGGREGATED HOLDINGS ===
detailed = df_merged[["Name", "Ticker", "Holding Value ($)"]]
aggregated_all = (
    detailed.groupby("Name")["Holding Value ($)"]
    .sum()
    .reset_index()
    .rename(columns={"Holding Value ($)": "Total Holding Value ($)"})
    .sort_values(by="Total Holding Value ($)", ascending=False)
)
detailed_all = detailed.merge(aggregated_all, on="Name")

# === FILTER FOR INSTITUTIONAL HOLDERS ===
detailed_filtered = detailed_all[detailed_all["Name"].str.lower().str.contains(pattern)]
aggregated_filtered = (
    detailed_filtered.groupby("Name")["Holding Value ($)"]
    .sum()
    .reset_index()
    .rename(columns={"Holding Value ($)": "Total Holding Value ($)"})
    .sort_values(by="Total Holding Value ($)", ascending=False)
)

# === SAVE CSVs ===
detailed_all.to_csv("All_Holder_Ticker_Holdings_ALL.csv", index=False)
aggregated_all.to_csv("All_Holder_Total_Holdings_ALL.csv", index=False)
detailed_filtered.to_csv("All_Holder_Ticker_Holdings_FILTERED.csv", index=False)
aggregated_filtered.to_csv("All_Holder_Total_Holdings_FILTERED.csv", index=False)

print("✅ Saved CSVs.")

# === SAVE TO MULTI-SHEET EXCEL ===
with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
    df_companies.to_excel(writer, sheet_name="Filtered Companies", index=False)
    detailed_all.to_excel(writer, sheet_name="All Holdings", index=False)
    aggregated_all.to_excel(writer, sheet_name="All Totals", index=False)
    detailed_filtered.to_excel(writer, sheet_name="Institutional Holdings", index=False)
    aggregated_filtered.to_excel(writer, sheet_name="Institutional Totals", index=False)

print(f"📁 Excel workbook saved: {excel_path}")

# === HEATMAP: Institutional Holders vs Tickers ($M) ===
pivot_df = detailed_filtered.pivot_table(
    index="Name", columns="Ticker", values="Holding Value ($)", aggfunc="sum", fill_value=0
)
pivot_df_millions = pivot_df / 1_000_000  # Convert to millions

plt.figure(figsize=(14, 8))
ax = sns.heatmap(
    pivot_df_millions,
    cmap="rocket_r",
    linewidths=0.5,
    cbar_kws={"label": "Holding Value ($M)"}
)
plt.title("Institutional Holders vs ASX Tickers ($M)")
plt.xlabel("ASX Ticker")
plt.ylabel("Shareholder")

# Format colorbar ticks
colorbar = ax.collections[0].colorbar
colorbar.ax.tick_params(labelsize=10)
colorbar.set_ticks(colorbar.get_ticks())
colorbar.set_ticklabels([f"${x:.2f}M" for x in colorbar.get_ticks()])

plt.tight_layout()
plt.show()
