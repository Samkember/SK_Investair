import pandas as pd
from sqlalchemy import create_engine

# === 1. Load CSV ===
csv_path = r'C:\Users\HarryBox\Documents\Full ASX Snapshot Retry\ASX_combined_final.csv'
df = pd.read_csv(csv_path)

# === 2. Rename columns to match SQL table ===
df.rename(columns={
    'Market Cap': 'MarketCap',
    'Shares Outstanding': 'SharesOutstanding',
    'Change (%)': 'ChangePercent',
    '1 Month Return (%)': 'Return1Month',
    '3 Month Return (%)': 'Return3Month',
    '6 Month Return (%)': 'Return6Month',
    '12 Month Return (%)': 'Return12Month'
}, inplace=True)

# === 3. Create SQLAlchemy engine ===
engine = create_engine(
    'mysql+pymysql://sam:sam2025@database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/ASX_Market'
)

# === 4. Upload to MySQL ===
df.to_sql(name='ASX_Data', con=engine, if_exists='append', index=False)

print("✅ Upload complete!")
