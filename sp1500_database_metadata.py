import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time

DB_URL = 'postgresql://postgres:sarina00@127.0.0.1:5431/sp1500_1d'
engine = create_engine(DB_URL)

def get_ticker_static_info(ticker):
    """Fetches only non-financial, static metadata with rate-limiting protection."""
    time.sleep(0.5) # Increased slightly to be extra safe after your previous block

    try:
        t = yf.Ticker(ticker)
        info = t.info
        if not info: return None

        return {
            'ticker': ticker.upper(),
            'name': info.get('longName'),
            'sector': info.get('sector'),
            'industry': info.get('industry'),
            'market_cap': info.get('marketCap'),
            'beta': info.get('beta')
        }
    except Exception:
        return None

def build_metadata_table():
    # 1. Get all tickers that should be in the DB
    with engine.connect() as conn:
        query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        all_tickers = [row[0].upper() for row in conn.execute(query)
                       if row[0] not in ['stock_metadata', 'stock_financials_quarterly']]

    # 2. Check which tickers we ALREADY have in the metadata table
    existing_tickers = []
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT ticker FROM stock_metadata"))
            existing_tickers = [row[0].upper() for row in res]
        print(f"📂 Found {len(existing_tickers)} stocks already processed. Resuming...")
    except Exception:
        print("📂 No existing metadata table found. Starting fresh.")

    # 3. Filter the list to only include MISSING tickers
    tickers_to_process = [t for t in all_tickers if t not in existing_tickers]

    if not tickers_to_process:
        print("✅ All tickers are already up to date!")
        return

    print(f"📡 Processing {len(tickers_to_process)} remaining stocks...")

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(tqdm(executor.map(get_ticker_static_info, tickers_to_process), total=len(tickers_to_process)))

    # 4. Save the new data (Append mode so we don't delete the 901 old ones)
    new_data = [r for r in results if r is not None]
    if new_data:
        df_new = pd.DataFrame(new_data)
        # We use 'append' here to add to the existing 901 rows
        df_new.to_sql('stock_metadata', engine, if_exists='append', index=False)

        # 5. Finalize the table (Ensure Primary Key exists)
        try:
            with engine.connect() as conn:
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute(text('ALTER TABLE stock_metadata ADD PRIMARY KEY (ticker);'))
        except Exception:
            pass # PK might already exist if it was a resume

        print(f"✅ Added {len(df_new)} new records. Metadata table is complete!")
    else:
        print("⚠️ No new data was captured.")

if __name__ == "__main__":
    build_metadata_table()
