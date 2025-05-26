from sqlalchemy import create_engine, text

# === MySQL connection ===
def get_mysql_engine():
    mysql_url = (
        "mysql+pymysql://sam:sam2025@"
        "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com:3306/Substantial_Holding"
    )
    return create_engine(mysql_url)

# # === Table creation SQL ===
# def create_table():
#     ddl = """
#     CREATE TABLE IF NOT EXISTS ASX_Annoucement_HeaderFiles (
#         id INT AUTO_INCREMENT PRIMARY KEY,
#         filename VARCHAR(255) UNIQUE,
#         announcement_number VARCHAR(255),
#         asx_code VARCHAR(10),
#         exchange VARCHAR(10),
#         `sensitive` VARCHAR(5),
#         headline TEXT,
#         rec_type VARCHAR(5),
#         rec_date DATE,
#         rec_time TIME,
#         rel_date DATE,
#         rel_time TIME,
#         n_pages INT,
#         rep_types TEXT
#     );
#     """
#     engine = get_mysql_engine()
#     with engine.begin() as conn:
#         conn.execute(text(ddl))
#     print("✅ Table 'asx_announcements' created successfully (or already exists).")


def drop_table():
    engine = get_mysql_engine()
    drop_sql = "DROP TABLE IF EXISTS asx_announcements;"
    
    with engine.begin() as conn:
        conn.execute(text(drop_sql))
    print("✅ Table 'asx_announcements' dropped successfully (if it existed).")


# === Entry point ===
if __name__ == "__main__":
    # create_table()
    drop_table()
