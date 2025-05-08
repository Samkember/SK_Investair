import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# === FILE PATHS ===
all_asx_path = r"C:\Users\HarryBox\Documents\Full ASX Snapshot\asx_combined_final.csv"  # Use ALL ASX data
substantial_path = r"C:\Users\HarryBox\Documents\all_substantial_shareholders.csv"
buyers_path = r"C:\Users\HarryBox\Documents\all_shareholders_buying.csv"
sellers_path = r"C:\Users\HarryBox\Documents\all_shareholders_selling.csv"
excel_output_path = "All_ASX_Holder_Heatmaps.xlsx"

# === LOAD DATA ===
df_companies = pd.read_csv(all_asx_path)
df_substantial = pd.read_csv(substantial_path)
df_buy = pd.read_csv(buyers_path)
df_sell = pd.read_csv(sellers_path)

# === CLEANING ===
for df in [df_companies, df_substantial, df_buy, df_sell]:
    df.columns = df.columns.str.strip()
    if "Ticker" in df.columns:
        df["Ticker"] = df["Ticker"].str.strip().str.upper()

# === DETECT SHARES HELD COLUMN ===
shares_col = next((col for col in df_substantial.columns if "share" in col.lower()), None)
if not shares_col:
    raise ValueError("❌ Could not find a 'Shares Held' column.")

# === MERGE PRICE DATA ===
df_merged = df_substantial.merge(df_companies[["Ticker", "Close"]], on="Ticker", how="left")
df_merged[shares_col] = df_merged[shares_col].astype(str).str.replace(",", "").astype(float)
df_merged["Close"] = pd.to_numeric(df_merged["Close"], errors="coerce")
df_merged = df_merged[df_merged["Close"] >= 0.01]
df_merged["Holding Value ($)"] = df_merged[shares_col] * df_merged["Close"]

# === FILTER FOR INSTITUTIONAL HOLDERS ===
keywords = ["fund", "management", "asset", "partners", "capital", "advisors", "ventures", "trading", "investment", "holdings"]
pattern = '|'.join(keywords)
df_merged_filtered = df_merged[df_merged["Name"].str.lower().str.contains(pattern, na=False)]

# === PIVOT HEATMAP DATA ===
holder_pivot = df_merged_filtered.pivot_table(index="Name", columns="Ticker", values="Holding Value ($)", aggfunc="sum", fill_value=0)
holder_pivot_m = holder_pivot / 1_000_000

buyer_pivot = None
if "Shares Bought" in df_buy.columns:
    df_buy["Ticker"] = df_buy["Ticker"].str.strip().str.upper()
    buyer_pivot = df_buy.pivot_table(index="Name", columns="Ticker", values="Shares Bought", aggfunc="sum", fill_value=0)

seller_pivot = None
if "Shares Sold" in df_sell.columns:
    df_sell["Ticker"] = df_sell["Ticker"].str.strip().str.upper()
    seller_pivot = df_sell.pivot_table(index="Name", columns="Ticker", values="Shares Sold", aggfunc="sum", fill_value=0)

# === SAVE TO EXCEL ===
with pd.ExcelWriter(excel_output_path, engine="xlsxwriter") as writer:
    df_companies.to_excel(writer, sheet_name="All ASX Companies", index=False)
    df_merged_filtered.to_excel(writer, sheet_name="Merged Holdings", index=False)
    holder_pivot_m.to_excel(writer, sheet_name="Substantial Heatmap ($M)")
    if buyer_pivot is not None:
        buyer_pivot.to_excel(writer, sheet_name="Buyers Pivot")
    if seller_pivot is not None:
        seller_pivot.to_excel(writer, sheet_name="Sellers Pivot")

print(f"✅ Excel workbook saved to {excel_output_path}")

# === HEATMAP: Substantial Holders ===
plt.figure(figsize=(16, 10))
ax1 = sns.heatmap(holder_pivot_m, cmap="rocket_r", linewidths=0.5, cbar_kws={"label": "Holding Value ($M)"})
plt.title("🔹 Institutional Holders vs ASX Tickers (Holding Value $M)")
plt.xlabel("ASX Ticker")
plt.ylabel("Holder")
cb = ax1.collections[0].colorbar
cb.set_ticklabels([f"${x:.2f}M" for x in cb.get_ticks()])
plt.tight_layout()
plt.show()
