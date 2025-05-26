import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from fuzzywuzzy import fuzz
from collections import defaultdict
import numpy as np

# Load Excel file
file_path = r'C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms\ForTim26052025\SS_CapitalGoodsSector.xlsx'
df = pd.read_excel(file_path)

# Filter only 'new' shareholders
new_shareholders = df[df['Last Movement'].str.contains('new', case=False, na=False)].copy()

# Normalize name casing for fuzzy matching
new_shareholders['Name_Lower'] = new_shareholders['Name'].str.lower()

# Step 1: Cluster similar names using fuzzy matching
unique_names = new_shareholders['Name_Lower'].dropna().unique()
name_groups = {}
used_names = set()

for name in unique_names:
    if name in used_names:
        continue
    matches = [other for other in unique_names if fuzz.ratio(name, other) >= 60]
    representative = sorted(matches, key=lambda x: -len(x))[0]
    for match in matches:
        name_groups[match] = representative
        used_names.add(match)

# Apply fuzzy grouping
new_shareholders['Normalized Name'] = new_shareholders['Name_Lower'].map(name_groups)

# Step 2: Count companies per normalized name
company_counts = new_shareholders.groupby('Normalized Name')['Ticker'].nunique()
names_in_multiple_companies = company_counts[company_counts >= 2].index

# Step 3: Filter only those shareholders
filtered_df = new_shareholders[new_shareholders['Normalized Name'].isin(names_in_multiple_companies)]

# Step 4: Create heatmap data for % Held
heatmap_data = filtered_df.pivot_table(
    index='Normalized Name',
    columns='Ticker',
    values='Shares Held (%)',
    aggfunc='mean',
    fill_value=0
)

# Step 5: Plot heatmap (Percentage Held, not log-scaled)
plt.figure(figsize=(18, 12))
sns.heatmap(
    heatmap_data,
    cmap='Blues',
    linewidths=0.3,
    linecolor='lightgray',
    cbar_kws={'label': '% Held'}
)
plt.title('Capital Goods Sector New Substaintial Holders (Past 12 Months) of 2 or More Companies', fontsize=16)
plt.xlabel('ASX Code', fontsize=12)
plt.ylabel('Shareholder Name', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
