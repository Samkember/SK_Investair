import pandas as pd
from sqlalchemy import create_engine

DB_HOST = "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com"
DB_USER = "sam"
DB_PASSWORD = "sam2025"
DB_NAME = "ASX_Market"
DB_PORT = int(3306)  # Default MySQL port
DB_CONNECTION_STRING = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# === CONFIG ===
SCHEMA_NAME = "ASX_Market"
TABLE_NAME = "ASX_Data"
CSV_OUTPUT = "ASX_OUTPUT.csv"

def get_mysql_engine(mysql_url=None):
    if mysql_url is None:
        mysql_url = (
            "mysql+pymysql://sam:sam2025@"
            "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/{schema_name}"
        ).format(schema_name=SCHEMA_NAME)
    return create_engine(mysql_url)

def export_table_to_csv():
    engine = get_mysql_engine()
    query = f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME} WHERE `Info Date` = '2025-05-29';"
    df = pd.read_sql(query, engine)
    df.to_csv(CSV_OUTPUT, index=False)
    print(f"✅ Exported {len(df)} rows to {CSV_OUTPUT}")

if __name__ == "__main__":
    export_table_to_csv()
