import pandas as pd
import matplotlib.pyplot as plt

# === FILE PATHS ===
buyers_path = r"C:\Users\HarryBox\Documents\all_shareholders_buying.csv"
sellers_path = r"C:\Users\HarryBox\Documents\all_shareholders_selling.csv"

# === LOAD DATA ===
df_buy = pd.read_csv(buyers_path)
df_sell = pd.read_csv(sellers_path)

# === HANDLE BUYERS ===
if 'Shares Bought' in df_buy.columns:
    top_buyers = df_buy.groupby('Name')['Shares Bought'].sum().sort_values(ascending=False).head(20)
    buy_label = "Shares Bought"
else:
    top_buyers = df_buy['Name'].value_counts().head(40)
    buy_label = "Number of Buys"

# === HANDLE SELLERS ===
if 'Shares Sold' in df_sell.columns:
    top_sellers = df_sell.groupby('Name')['Shares Sold'].sum().sort_values(ascending=False).head(20)
    sell_label = "Shares Sold"
else:
    top_sellers = df_sell['Name'].value_counts().head(40)
    sell_label = "Number of Sells"

# === PLOT SIDE BY SIDE ===
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8), sharey=False)

# Buyers
top_buyers.plot(kind='barh', color='seagreen', ax=ax1)
ax1.set_title("Top 20 Buyers of ASX Stocks")
ax1.set_xlabel(buy_label)
ax1.set_ylabel("Shareholder")
ax1.invert_yaxis()

# Sellers
top_sellers.plot(kind='barh', color='tomato', ax=ax2)
ax2.set_title("Top 20 Sellers of ASX Stocks")
ax2.set_xlabel(sell_label)
ax2.set_ylabel("Shareholder")
ax2.invert_yaxis()

# Layout
plt.tight_layout()
plt.show()
