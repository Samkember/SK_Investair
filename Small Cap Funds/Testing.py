import pandas as pd
import matplotlib.pyplot as plt

# === FILE PATHS ===
substantial_path = r"C:\Users\HarryBox\Documents\all_substantial_shareholders.csv"
buyers_path = r"C:\Users\HarryBox\Documents\all_shareholders_buying.csv"
sellers_path = r"C:\Users\HarryBox\Documents\all_shareholders_selling.csv"

# === LOAD DATA ===
df_substantial = pd.read_csv(substantial_path)
df_buy = pd.read_csv(buyers_path)
df_sell = pd.read_csv(sellers_path)

# === REMOVE "State Street" FROM ALL ===
df_substantial = df_substantial[~df_substantial['Name'].str.contains("state street", case=False, na=False)]
df_buy = df_buy[~df_buy['Name'].str.contains("state street", case=False, na=False)]
df_sell = df_sell[~df_sell['Name'].str.contains("state street", case=False, na=False)]

# === TOP SHAREHOLDERS BY NUMBER OF COMPANIES HELD ===
share_counts = df_substantial.groupby('Name')['Ticker'].nunique().sort_values(ascending=False)
top_shareholders = share_counts[share_counts > 1].head(40)

# === TOP BUYERS ===
if 'Shares Bought' in df_buy.columns:
    top_buyers = df_buy.groupby('Name')['Shares Bought'].sum().sort_values(ascending=False).head(20)
    buy_label = "Shares Bought"
else:
    top_buyers = df_buy['Name'].value_counts().head(40)
    buy_label = "Number of Companies Bought During Period"

# === TOP SELLERS ===
if 'Shares Sold' in df_sell.columns:
    top_sellers = df_sell.groupby('Name')['Shares Sold'].sum().sort_values(ascending=False).head(20)
    sell_label = "Shares Sold"
else:
    top_sellers = df_sell['Name'].value_counts().head(40)
    sell_label = "Number of Companies Sold During Period"

# === PLOT ALL THREE SIDE BY SIDE ===
fig, axs = plt.subplots(1, 3, figsize=(22, 8), sharey=False)

# Plot Substantial Holders
top_shareholders.plot(kind='barh', color='steelblue', ax=axs[0])
axs[0].set_title("Top Shareholders by Companies Held")
axs[0].set_xlabel("Number of Substantial Ownerships in Companies")
axs[0].set_ylabel("Fund / Holder")
axs[0].invert_yaxis()

# Plot Buyers
top_buyers.plot(kind='barh', color='seagreen', ax=axs[1])
axs[1].set_title("Top Substantial Buyers")
axs[1].set_xlabel(buy_label)
axs[1].set_ylabel("Shareholder")
axs[1].invert_yaxis()

# Plot Sellers
top_sellers.plot(kind='barh', color='tomato', ax=axs[2])
axs[2].set_title("Top Substantial Sellers")
axs[2].set_xlabel(sell_label)
axs[2].set_ylabel("Shareholder")
axs[2].invert_yaxis()

plt.tight_layout()
plt.show()
