from sqlalchemy import create_engine, text
import os, sys
import pandas as pd

outputFolderPath = r"C:\Users\HarryBox\Documents\SK_Investair\ASX_SS_Forms\ASX_SS_Forms_2\Output"

# === MySQL connection ===
def get_mysql_engine():
    mysql_url = (
        "mysql+pymysql://sam:sam2025@"
        "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/Substantial_Holding"
    )
    return create_engine(mysql_url)

# === Table creation SQL ===
def create_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS ASX_Annoucement_HeaderFiles (
        filename VARCHAR(255) UNIQUE,
        announcement_number VARCHAR(255),
        asx_code VARCHAR(10),
        exchange VARCHAR(10),
        `sensitive` VARCHAR(5),
        headline TEXT,
        rec_type VARCHAR(5),
        rec_date DATE,
        rec_time TIME,
        rel_date DATE,
        rel_time TIME,
        n_pages INT,
        rep_types TEXT
    );
    """
    engine = get_mysql_engine()
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("✅ Table 'ASX_Annoucement_HeaderFiles' created successfully (or already exists).")


def drop_table():
    engine = get_mysql_engine()
    drop_sql = "DROP TABLE IF EXISTS ASX_Annoucement_HeaderFiles;"
    
    with engine.begin() as conn:
        conn.execute(text(drop_sql))
    print("✅ Table 'asx_announcements' dropped successfully (if it existed).")

def export_sql_to_csv():
    engine = get_mysql_engine()
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", engine)
    csv_path = os.path.join(outputFolderPath, f"{TABLE_NAME}_export.csv")
    df.to_csv(csv_path, index=False)
    print(f"✅ Exported table to {csv_path}")



# === Entry point ===
if __name__ == "__main__":
    # === Configurable schema and table name ===
    schema_name = "Substantial_Holding"
    table_name = "ASX_Annoucement_HeaderFiles"

    # Apply to global table usage
    TABLE_NAME = table_name

    # Call the function you want here
    # create_table()
    # drop_table()
    # export_sql_to_csv()
