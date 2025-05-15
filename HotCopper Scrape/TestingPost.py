import pandas as pd
import os
import re

# === CONFIG ===
SAVE_DIR = r"C:\Users\HarryBox\Documents\Investair\HotCopper Scrape"
INPUT_CSV = os.path.join(SAVE_DIR, "latest_posts_cleaned.csv")
OUTPUT_CSV = os.path.join(SAVE_DIR, "latest_posts_ASX_only.csv")

# === LOAD ===
df = pd.read_csv(INPUT_CSV)

# === CLEAN WHITESPACE ===
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# === HELPER: Is ASX Code ===
def is_asx_code(tag):
    return isinstance(tag, str) and re.match(r'^[A-Z]{2,5}$', tag) is not None

# === FILTER ===
df_filtered = df[
    df['Tag'].apply(is_asx_code) & 
    df['Views'].notnull() & 
    (df['Views'] != '')
]

# === SAVE ===
os.makedirs(SAVE_DIR, exist_ok=True)
df_filtered.to_csv(OUTPUT_CSV, index=False)

print(f"✅ Filtered ASX-only posts saved to: {OUTPUT_CSV}")
