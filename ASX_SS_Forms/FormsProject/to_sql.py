import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, text

# Step 1: Load CSV
csv_path = r'path_to_your_file.csv'  # Update this to your actual path
holdings_df = pd.read_csv(csv_path)

# Step 2: Upload to MySQL
try:
    # Convert date column to proper format
    holdings_df["Date"] = pd.to_datetime(holdings_df["Date"]).dt.date
    upload_date = holdings_df["Date"].min()

    # SQL connection
    engine = create_engine(
        'mysql+pymysql://sam:sam2025@database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/ASX_Market'
    )
    meta = MetaData()

    with engine.begin() as conn:
        if not engine.dialect.has_table(conn, "ASX_RepTypes", schema="ASX_Market"):
            rep_table = Table(
                'ASX_RepTypes', meta,
                Column('Filename', String(50)),
                Column('RepType Code', String(10)),
                Column('Description', String(100)),
                Column('Ticker', String(10)),
                Column('Date', Date),
                schema='ASX_Market'
            )
            meta.create_all(engine)
            print("📋 Created SQL table 'ASX_RepTypes' in schema 'ASX_Market'.")

        # Delete existing rows for the same date
        delete_query = text("""
            DELETE FROM ASX_Market.ASX_RepTypes
            WHERE `Date` = :upload_date
        """)
        conn.execute(delete_query, {"upload_date": upload_date})
        print(f"🧹 Removed existing entries for date {upload_date}")

    # Upload new data
    with engine.begin() as conn:
        holdings_df.to_sql(
            name='ASX_RepTypes',
            con=conn,
            if_exists='append',
            index=False,
            schema='ASX_Market'
        )
        print("✅ Upload to SQL complete.")

except Exception as e:
    print(f"❌ Failed to upload to SQL: {e}")
