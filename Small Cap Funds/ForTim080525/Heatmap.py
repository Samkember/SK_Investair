import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# File path to CSV
filePath = r'/Users/samkember/Documents/SK_Investair/Small Cap Funds/ForTim080525/all_substantial_shareholders.csv'

# Load CSV
df = pd.read_csv(filePath)

# Select and rename relevant columns
df = df[['Name', 'Ticker', 'Shares Held (%)']]
df.columns = ['Holder', 'Stock', 'PercentHeld']

# Convert percentage values to numeric
df['PercentHeld'] = pd.to_numeric(df['PercentHeld'], errors='coerce')

# Drop invalid or missing rows
df = df.dropna(subset=['Holder', 'Stock', 'PercentHeld'])

# # Use all holders
# filtered_df = df.copy()

# Filter to holders with interest in 2 or more different stocks
holder_counts = df.groupby('Holder')['Stock'].nunique()
multi_stock_holders = holder_counts[holder_counts >= 2].index
filtered_df = df[df['Holder'].isin(multi_stock_holders)]

# Create pivot table: rows = holders, columns = stocks, values = % held
pivot_df = filtered_df.pivot_table(
    index="Holder", columns="Stock", values="PercentHeld", aggfunc="sum", fill_value=0
)

# Plot heatmap
plt.figure(figsize=(22, 10))  # Wider plot for 75 tickers

# Add top info strip
plt.suptitle("Capital Goods Substantial Shareholders in the Sector",
             fontsize=14, fontweight='bold', y=0.96)

# Generate the heatmap
ax = sns.heatmap(
    pivot_df,
    cmap="rocket_r",
    linewidths=0.5,
    cbar_kws={"label": "Ownership (%)"}
)

# Rotate x-axis labels to make tickers readable
plt.xticks(rotation=90)

# Add title and labels
plt.title("Institutional Holders vs ASX Tickers (% Ownership)", fontsize=12, pad=10)
plt.xlabel("ASX Ticker")
plt.ylabel("Shareholder")

# Format colorbar ticks
colorbar = ax.collections[0].colorbar
colorbar.ax.tick_params(labelsize=10)
colorbar.set_ticks(colorbar.get_ticks())
colorbar.set_ticklabels([f"{x:.2f}%" for x in colorbar.get_ticks()])

# Adjust layout to fit everything
plt.tight_layout(rect=[0, 0, 1, 0.94])
plt.show()
