import pandas as pd
from sqlalchemy import create_engine, text

# === DATABASE CONFIGURATION ===
DB_HOST = "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com"
DB_USER = "sam"
DB_PASSWORD = "sam2025"
DB_NAME = "ASX_Market"
DB_PORT = 3306
DB_CONNECTION_STRING = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# === TABLE & SCHEMA NAMES ===
SCHEMA_NAME = "ASX_Market"
COMPANY_CODES_TABLE = "ASX_Company_Codes"
SUB_HOLDINGS_TABLE = "Substantial_Holding.reconciled"


def get_mysql_engine(mysql_url: str | None = None):
    """Create and return a SQLAlchemy engine."""
    if mysql_url is None:
        mysql_url = DB_CONNECTION_STRING
    return create_engine(mysql_url)


def get_sector_tickers(engine, sector: str) -> list[str]:
    """Retrieve tickers for a given sector."""
    query = text(
        f"""
        SELECT ticker
        FROM {SCHEMA_NAME}.{COMPANY_CODES_TABLE}
        WHERE Sector = :sector;
        """
    )
    df = pd.read_sql(query, engine, params={"sector": sector})
    return df["ticker"].tolist()


def fetch_substantial_holdings(engine, tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch substantial holding records for a list of tickers in a date range."""
    if not tickers:
        print("⚠️  No tickers found for the specified sector.")
        return pd.DataFrame()

    # Create dynamic named placeholders
    bind_names = [f":ticker_{i}" for i in range(len(tickers))]
    bind_clause = ",".join(bind_names)

    query = text(
        f"""
        SELECT id,
               shareholder,
               ticker,
               event_date,
               event_type,
               prev_voting_power,
               post_voting_power
        FROM {SUB_HOLDINGS_TABLE}
        WHERE ticker IN ({bind_clause})
          AND event_date BETWEEN :start_date AND :end_date;
        """
    )

    # Combine all bind parameters
    bind_params = {f"ticker_{i}": ticker for i, ticker in enumerate(tickers)}
    bind_params["start_date"] = start_date
    bind_params["end_date"] = end_date

    return pd.read_sql(query, engine, params=bind_params)


def main(sector_name: str, start_date: str, end_date: str):
    engine = get_mysql_engine()

    # 1) Fetch tickers for specified sector
    tickers = get_sector_tickers(engine, sector_name)

    # 2) Query substantial holdings
    holdings_df = fetch_substantial_holdings(engine, tickers, start_date, end_date)

    # 3) Display results
    if holdings_df.empty:
        print(f"No substantial-holding entries found for sector '{sector_name}' between {start_date} and {end_date}.")
    else:
        print(holdings_df)
        print(f"\n✅ {len(holdings_df)} rows returned.")

    engine.dispose()


if __name__ == "__main__":
    # Example: Capital Goods sector, week of May 5–9, 2025
    main(sector_name="Capital Goods", start_date="2025-05-05", end_date="2025-05-09")
