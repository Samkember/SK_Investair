import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from fuzzywuzzy import fuzz
import numpy as np

# Load Excel file
file_path = r'C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms\ForTim26052025\SS_CapitalGoodsSector.xlsx'
df = pd.read_excel(file_path)

# Filter only shareholders whose last movement was 'cease'
ceased_shareholders = df[df['Last Movement'].str.contains('cease', case=False, na=False)].copy()

# Normalize name casing for fuzzy matching
ceased_shareholders['Name_Lower'] = ceased_shareholders['Name'].str.lower()

# Step 1: Fuzzy group names
unique_names = ceased_shareholders['Name_Lower'].dropna().unique()
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

# Apply grouping
ceased_shareholders['Normalized Name'] = ceased_shareholders['Name_Lower'].map(name_groups)

# Step 2: Find those in 2+ companies
company_counts = ceased_shareholders.groupby('Normalized Name')['Ticker'].nunique()
names_in_multiple_ceased = company_counts[company_counts >= 2].index

# Step 3: Filter those names
filtered_ceased = ceased_shareholders[ceased_shareholders['Normalized Name'].isin(names_in_multiple_ceased)]

# Step 4: Create binary presence matrix (1 = ceased)
binary_heatmap_data = filtered_ceased.pivot_table(
    index='Normalized Name',
    columns='Ticker',
    values='Name',
    aggfunc='count',
    fill_value=0
)
binary_heatmap_data[binary_heatmap_data > 0] = 1  # Ensure binary values

# Step 5: Plot heatmap (binary)
plt.figure(figsize=(18, 12))
sns.heatmap(
    binary_heatmap_data,
    cmap='Reds',  # Red intensity for exits
    linewidths=0.3,
    linecolor='gray',
    cbar_kws={'label': 'Ceased (1 = Exit)'}
)
plt.title('Capital Goods Sector Substaintial Holders who Ceased to be SS in 2 or more Companies (Past 12 Months)', fontsize=16)
plt.xlabel('ASX Code', fontsize=12)
plt.ylabel('Shareholder Name', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
