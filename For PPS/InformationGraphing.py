import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# === CONFIG ===
OUTPUT_FOLDER = r"C:\Users\HarryBox\Documents\Investair\For PPS\CSVOutput"
input_file = os.path.join(OUTPUT_FOLDER, "software_services_substantial_shareholders.csv")
company_list_file = os.path.join(OUTPUT_FOLDER, "asx_software_services_100m_to_1b.csv")

# === STEP 1: Load the data ===
df = pd.read_csv(input_file)
df_companies = pd.read_csv(company_list_file)

# === STEP 2: Clean the Shares Held (%) Column ===
df["Shares Held (%)"] = (
    df["Shares Held (%)"]
    .astype(str)
    .str.replace("%", "", regex=False)
    .replace("nan", "0")
    .astype(float)
)

# === STEP 3: Filter shareholders based on investment fund-related keywords ===
investment_keywords = ["fund", "capital", "management", "partners", "investments", "holdings", "asset", "group"]

def is_investment_related(name):
    name_lower = str(name).lower()
    return any(keyword in name_lower for keyword in investment_keywords)

df_filtered = df[df["Name"].apply(is_investment_related)]

# === STEP 4: Pivot the data ===
pivot_df_all = df_filtered.pivot_table(
    index="Name",
    columns="Ticker",
    values="Shares Held (%)",
    aggfunc="sum"
).fillna(0)

# Keep only companies in your 35 list
all_tickers = df_companies["Ticker"].dropna().unique().tolist()
pivot_df_all = pivot_df_all.reindex(columns=all_tickers, fill_value=0)

# === STEP 5: Make 2 versions of the pivot ===

# --- (1) Funds invested in multiple companies ---
pivot_df_multi = pivot_df_all.copy()
fund_holdings_count = (pivot_df_multi > 0).sum(axis=1)
pivot_df_multi = pivot_df_multi[fund_holdings_count > 1]
ticker_holdings_count = (pivot_df_multi > 0).sum(axis=0)
pivot_df_multi = pivot_df_multi.loc[:, ticker_holdings_count > 0]

# --- (2) All funds (regardless of number of holdings) ---
pivot_df_all_active = pivot_df_all.loc[:, (pivot_df_all != 0).any(axis=0)]

# === STEP 6: Plot first heatmap (Multi-fund investors) ===
annotations_multi = pivot_df_multi.applymap(lambda x: f"{x:.1f}" if x > 0 else "")
mask_multi = pivot_df_multi == 0

plt.figure(figsize=(max(20, len(pivot_df_multi.columns) * 0.8), 14))
sns.heatmap(
    pivot_df_multi,
    cmap="viridis",
    linewidths=0.5,
    linecolor='gray',
    cbar_kws={'label': '% Shares Held'},
    annot=annotations_multi,
    fmt="",
    annot_kws={"size": 7},
    mask=mask_multi,
    square=False
)
plt.title("Investment Funds Holding Multiple Companies", fontsize=20)
plt.xlabel("Company (Ticker)", fontsize=14)
plt.ylabel("Investment Funds", fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()
multi_save_path = os.path.join(OUTPUT_FOLDER, "investment_funds_multi_company_heatmap.pdf")
plt.savefig(multi_save_path, format="pdf")  # <-- SAVE AS PDF
plt.show()

# === STEP 7: Plot second heatmap (All active funds) ===
annotations_all = pivot_df_all_active.applymap(lambda x: f"{x:.1f}" if x > 0 else "")
mask_all = pivot_df_all_active == 0

plt.figure(figsize=(max(20, len(pivot_df_all_active.columns) * 0.8), 14))
sns.heatmap(
    pivot_df_all_active,
    cmap="viridis",
    linewidths=0.5,
    linecolor='gray',
    cbar_kws={'label': '% Shares Held'},
    annot=annotations_all,
    fmt="",
    annot_kws={"size": 7},
    mask=mask_all,
    square=False
)
plt.title("All Investment Funds", fontsize=20)
plt.xlabel("Company (Ticker)", fontsize=14)
plt.ylabel("Investment Funds", fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.yticks(rotation=0)
plt.tight_layout()
all_save_path = os.path.join(OUTPUT_FOLDER, "investment_funds_all_funds_heatmap.pdf")
plt.savefig(all_save_path, format="pdf")  # <-- SAVE AS PDF
plt.show()

print(f"✅ Heatmaps saved as PDFs:\n- Multi-investor funds: {multi_save_path}\n- All active funds: {all_save_path}")
