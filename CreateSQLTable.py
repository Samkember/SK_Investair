from sqlalchemy import create_engine, MetaData, Table, Column, String

# === CONFIG ===
SCHEMA_NAME = "ASX_Market"
TABLE_NAME = "ASX_Company_Codes"

# === CONNECT TO DATABASE ===
engine = create_engine(
    f'mysql+pymysql://sam:sam2025@database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/{SCHEMA_NAME}',
    connect_args={"connect_timeout": 10}
)
meta = MetaData()

# === DEFINE TABLE SCHEMA ===
asx_codes_table = Table(
    TABLE_NAME, meta,
    Column('Ticker', String(10), primary_key=True),
    Column('CompanyName', String(255)),
    Column('Sector', String(100)),
    schema=SCHEMA_NAME
)

# === CREATE TABLE ===
with engine.begin() as conn:
    if not engine.dialect.has_table(conn, TABLE_NAME, schema=SCHEMA_NAME):
        meta.create_all(engine)
        print(f"✅ Table '{TABLE_NAME}' created in schema '{SCHEMA_NAME}'.")
    else:
        print(f"ℹ️ Table '{TABLE_NAME}' already exists in schema '{SCHEMA_NAME}'.")
