import pandas as pd
import numpy as np
import json
import os
import pymysql
from sqlalchemy import create_engine, text
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# === THREAD CONFIGURATION ===
MAX_THREADS = 50

# === DATABASE CONFIGURATION ===
DB_HOST = "database-1.cmy0wo2batmu.ap-southeast-2.rds.amazonaws.com"
DB_USER = "sam"
DB_PASSWORD = "sam2025"
DB_NAME = "ASX_Market"
DB_PORT = 3306
DB_CONNECTION_STRING = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# === SIGNAL DETECTION FUNCTIONS ===
def annotate_signal_context(df, signal_rows):
    signal_rows = signal_rows.merge(df[['snapshot_date', 'close_price', 'volume']], on='snapshot_date', how='left')
    signal_rows['price_before'] = signal_rows['close_price'].shift(1)
    signal_rows['price_after'] = signal_rows['close_price'].shift(-1)
    signal_rows['volume_pct_change'] = df['volume'].pct_change().reindex(signal_rows.index)
    return signal_rows.drop(columns=['close_price', 'volume'])

def detect_increasing_volume_price_ema_signals(data: pd.DataFrame, ticker: str, fast_span=5, slow_span=20):
    df = data[data['ticker'] == ticker].copy()
    df = df.sort_values('snapshot_date')
    df['close_ema_fast'] = df['close_price'].ewm(span=fast_span, adjust=False).mean()
    df['close_ema_slow'] = df['close_price'].ewm(span=slow_span, adjust=False).mean()
    df['volume_ema_fast'] = df['volume'].ewm(span=fast_span, adjust=False).mean()
    df['volume_ema_slow'] = df['volume'].ewm(span=slow_span, adjust=False).mean()
    df['trigger'] = (df['close_ema_fast'] > df['close_ema_slow']) & (df['volume_ema_fast'] > df['volume_ema_slow'])
    df['price_diff'] = df['close_ema_fast'] - df['close_ema_slow']
    df['volume_diff'] = df['volume_ema_fast'] - df['volume_ema_slow']
    signals_df = df[df['trigger']][['snapshot_date', 'price_diff', 'volume_diff']].copy()
    signals_df['ticker'] = ticker
    signals_df['signal_type'] = 'bull_flag'
    signals_df['reason'] = signals_df.apply(lambda x: f"Price EMA fast > slow by {x['price_diff']:.2f}; Volume EMA fast > slow by {int(x['volume_diff'])}", axis=1)
    return annotate_signal_context(df, signals_df[['signal_type', 'ticker', 'snapshot_date', 'reason']])

def detect_increasing_volume_price_drop_ema_signals(data, ticker, fast_span=5, slow_span=20):
    df = data[data['ticker'] == ticker].copy()
    df = df.sort_values('snapshot_date')
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
    df['close_ema_fast'] = df['close_price'].ewm(span=fast_span, adjust=False).mean()
    df['close_ema_slow'] = df['close_price'].ewm(span=slow_span, adjust=False).mean()
    df['volume_ema_fast'] = df['volume'].ewm(span=fast_span, adjust=False).mean()
    df['volume_ema_slow'] = df['volume'].ewm(span=slow_span, adjust=False).mean()
    df['trigger'] = (df['close_ema_fast'] < df['close_ema_slow']) & (df['volume_ema_fast'] > df['volume_ema_slow'])
    df['price_diff'] = df['close_ema_slow'] - df['close_ema_fast']
    df['volume_diff'] = df['volume_ema_fast'] - df['volume_ema_slow']
    signals_df = df[df['trigger']][['snapshot_date', 'price_diff', 'volume_diff']].copy()
    signals_df['ticker'] = ticker
    signals_df['signal_type'] = 'bear_flag'
    signals_df['reason'] = signals_df.apply(lambda x: f"Price EMA fast < slow by {x['price_diff']:.2f}; Volume EMA fast > slow by {int(x['volume_diff'])}", axis=1)
    return annotate_signal_context(df, signals_df[['signal_type', 'ticker', 'snapshot_date', 'reason']])

def detect_price_reversal_signals(data, ticker, price_trend_window=5, min_trend=2, fast_span=5, slow_span=20, zscore_threshold=2.0, zscore_window=10):
    df = data[data['ticker'] == ticker].copy()
    df = df.sort_values('snapshot_date').reset_index(drop=True)
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
    df['close_ema_fast'] = df['close_price'].ewm(span=fast_span, adjust=False).mean()
    df['close_ema_slow'] = df['close_price'].ewm(span=slow_span, adjust=False).mean()
    df['trend'] = np.where(df['close_ema_fast'] > df['close_ema_slow'], 1, np.where(df['close_ema_fast'] < df['close_ema_slow'], -1, 0))
    df['volume_pct_change'] = df['volume'].pct_change()
    rolling_mean = df['volume_pct_change'].rolling(window=zscore_window)
    df['vol_zscore'] = (df['volume_pct_change'] - rolling_mean.mean()) / rolling_mean.std()
    df['vol_zscore'] = df['vol_zscore'].fillna(0)
    df['signal_type'] = None
    df['reason'] = None
    for i in range(price_trend_window, len(df)):
        trend_score = df.loc[i - price_trend_window:i - 1, 'trend'].sum()
        zscore = df.loc[i, 'vol_zscore']
        if trend_score >= min_trend and zscore > zscore_threshold:
            df.at[i, 'signal_type'] = 'bull_reversal'
            df.at[i, 'reason'] = f"Trend score +{trend_score}, Volume Z-score {zscore:.2f}"
        elif trend_score <= -min_trend and zscore > zscore_threshold:
            df.at[i, 'signal_type'] = 'bear_reversal'
            df.at[i, 'reason'] = f"Trend score {trend_score}, Volume Z-score {zscore:.2f}"
    signal_rows = df[df['signal_type'].notna()][['snapshot_date', 'signal_type', 'reason']].copy()
    signal_rows['ticker'] = ticker
    return annotate_signal_context(df, signal_rows[['signal_type', 'ticker', 'snapshot_date', 'reason']])

def detect_breakout_signals(data, ticker, window=20, volume_zscore_threshold=2.0, zscore_window=10):
    df = data[data['ticker'] == ticker].copy()
    df = df.sort_values('snapshot_date').reset_index(drop=True)
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
    df['range_high'] = df['close_price'].rolling(window=window).max().shift(1)
    df['range_low'] = df['close_price'].rolling(window=window).min().shift(1)
    df['volume_pct_change'] = df['volume'].pct_change()
    rolling_mean = df['volume_pct_change'].rolling(window=zscore_window)
    df['vol_zscore'] = (df['volume_pct_change'] - rolling_mean.mean()) / rolling_mean.std()
    df['vol_zscore'] = df['vol_zscore'].fillna(0)
    df['signal_type'] = None
    df['reason'] = None
    df.loc[(df['close_price'] > df['range_high']) & (df['vol_zscore'] > volume_zscore_threshold), 'signal_type'] = 'bull_breakout'
    df.loc[(df['close_price'] > df['range_high']) & (df['vol_zscore'] > volume_zscore_threshold), 'reason'] = df.apply(lambda row: f"Price {row['close_price']:.2f} broke above {row['range_high']:.2f}, Volume Z-score {row['vol_zscore']:.2f}", axis=1)
    df.loc[(df['close_price'] < df['range_low']) & (df['vol_zscore'] > volume_zscore_threshold), 'signal_type'] = 'bear_breakout'
    df.loc[(df['close_price'] < df['range_low']) & (df['vol_zscore'] > volume_zscore_threshold), 'reason'] = df.apply(lambda row: f"Price {row['close_price']:.2f} broke below {row['range_low']:.2f}, Volume Z-score {row['vol_zscore']:.2f}", axis=1)
    signal_rows = df[df['signal_type'].notna()][['snapshot_date', 'signal_type', 'reason']].copy()
    signal_rows['ticker'] = ticker
    return annotate_signal_context(df, signal_rows[['signal_type', 'ticker', 'snapshot_date', 'reason']])

# === SIGNAL WRAPPER ===
def get_signals(data, ticker):
    all_signals = [
        detect_increasing_volume_price_ema_signals(data, ticker),
        detect_increasing_volume_price_drop_ema_signals(data, ticker),
        detect_price_reversal_signals(data, ticker),
        detect_breakout_signals(data, ticker)
    ]
    all_signals = [df for df in all_signals if not df.empty]
    return pd.concat(all_signals, ignore_index=True) if all_signals else pd.DataFrame(columns=['signal_type', 'ticker', 'snapshot_date', 'reason', 'price_before', 'price_after', 'volume_pct_change'])

# === SAFE CONVERSION ===
def safe_sql(val):
    return None if pd.isna(val) else val

# === DATABASE WRITING ===
def process_ticker(ticker):
    try:
        print(f"[{threading.current_thread().name}] Processing ticker: {ticker}")
        conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT, cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT snapshot_date, close_price, volume, ticker FROM asx_market_snapshot
                WHERE ticker = %s ORDER BY snapshot_date ASC
            """, (ticker,))
            rows = cursor.fetchall()
        conn.close()

        if not rows:
            return {"ticker": ticker, "result": "No data found"}

        df = pd.DataFrame(rows)
        signals = get_signals(df, ticker)
        if signals.empty:
            return {"ticker": ticker, "attempted": 0, "inserted": 0, "skipped": 0, "status": "no signals"}

        signals.drop_duplicates(subset=["signal_type", "ticker", "snapshot_date"], inplace=True)

        engine = create_engine(DB_CONNECTION_STRING)
        inserted = 0
        with engine.begin() as conn:
            for _, row in signals.iterrows():
                result = conn.execute(
                    text("""
                    INSERT IGNORE INTO triggers5 (signal_type, ticker, snapshot_date, reason, price_before, price_after, volume_pct_change)
                    VALUES (:signal_type, :ticker, :snapshot_date, :reason, :price_before, :price_after, :volume_pct_change)
                    """),
                    {
                        "signal_type": row['signal_type'],
                        "ticker": row['ticker'],
                        "snapshot_date": row['snapshot_date'],
                        "reason": row['reason'],
                        "price_before": safe_sql(row['price_before']),
                        "price_after": safe_sql(row['price_after']),
                        "volume_pct_change": safe_sql(row['volume_pct_change'])
                    }
                )
                inserted += result.rowcount

        skipped = len(signals) - inserted
        counts = signals['signal_type'].value_counts().to_dict()
        return {"ticker": ticker, "attempted": len(signals), "inserted": inserted, "skipped": skipped, **counts}
    except Exception as e:
        return {"ticker": ticker, "error": str(e)}

# === MAIN EXECUTION ===
if __name__ == "__main__":
    engine = create_engine(DB_CONNECTION_STRING)
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS triggers5 (
            signal_type VARCHAR(50),
            ticker VARCHAR(10),
            snapshot_date DATE,
            reason TEXT,
            price_before FLOAT,
            price_after FLOAT,
            volume_pct_change FLOAT,
            PRIMARY KEY (signal_type, ticker, snapshot_date)
        )
        """))

    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT, cursorclass=pymysql.cursors.DictCursor)
    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT ticker FROM asx_market_snapshot")
        tickers = [row['ticker'] for row in cursor.fetchall()]
    conn.close()

    results = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_ticker, ticker): ticker for ticker in tickers}
        for future in as_completed(futures):
            results.append(future.result())

    print(json.dumps(results, indent=2))
